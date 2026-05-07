package com.say5.spark_evolve

import com.say5.spark_evolve.config.JobConfig
import com.say5.spark_evolve.ingest.{KafkaSource, KafkaStreamSource}
import com.say5.spark_evolve.obs.Metrics
import com.say5.spark_evolve.schema.{Compatibility, SchemaRegistry, Violation}
import com.say5.spark_evolve.transform.{
  Aggregator,
  IcebergSink,
  ParquetSink,
  StreamingParquetSink,
  StreamingValidator,
  Validator
}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/** Entry point. Two top-level sub-commands:
  *
  * `run` — execute the batch pipeline (Kafka → Parquet) `schema check` — validate that one schema may replace
  * another
  *
  * Both commands read configuration from `application.conf` for shared settings; flags override per-command.
  */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val exitCode = args.toList match {
      case "schema" :: "check" :: rest => runSchemaCheck(rest)
      case "run" :: _                  => runPipeline(); 0
      case "streaming" :: _            => runStreamingPipeline(); 0
      case Nil                         => runPipeline(); 0
      case "--help" :: _ | "-h" :: _   => printHelp(); 0
      case other =>
        Console.err.println(s"unknown command: ${other.mkString(" ")}")
        printHelp()
        2
    }
    if (exitCode != 0) sys.exit(exitCode)
  }

  private def printHelp(): Unit = {
    Console.out.println(
      """spark-evolve — Spark pipeline with codified schema-evolution rules.
        |
        |Commands:
        |  run                                                   Run the batch pipeline (default).
        |  streaming                                             Run the structured-streaming pipeline.
        |  schema check --old <path> --new <path> --level <lvl>  Check compatibility of two .avsc files.
        |                                                        Levels: backward, forward, full, none.
        |""".stripMargin
    )
  }

  private def runSchemaCheck(args: List[String]): Int = {
    val parsed   = parseFlags(args)
    val oldPath  = parsed.getOrElse("old", "")
    val newPath  = parsed.getOrElse("new", "")
    val levelStr = parsed.getOrElse("level", "backward")
    if (oldPath.isEmpty || newPath.isEmpty) {
      Console.err.println("schema check requires --old <path> and --new <path>")
      return 2
    }
    val level = Compatibility.CompatibilityLevel.parse(levelStr) match {
      case Right(l)  => l
      case Left(err) => Console.err.println(err); return 2
    }
    val oldS = SchemaRegistry.parseFile(oldPath).get
    val newS = SchemaRegistry.parseFile(newPath).get
    Compatibility.check(oldS, newS, level) match {
      case Right(_) =>
        Console.out.println("compatible")
        0
      case Left(violations) =>
        Console.out.println(s"incompatible at level=$levelStr:")
        Console.out.println(Violation.toMarkdown(violations))
        1
    }
  }

  private def parseFlags(args: List[String]): Map[String, String] = {
    val m    = scala.collection.mutable.Map.empty[String, String]
    var rest = args
    while (rest.nonEmpty) {
      rest match {
        case key :: value :: tail if key.startsWith("--") =>
          m.update(key.drop(2), value)
          rest = tail
        case _ :: tail =>
          rest = tail
        case Nil => ()
      }
    }
    m.toMap
  }

  private def runPipeline(): Unit = {
    val cfg = JobConfig.load()
    log.info(s"loaded config: kafka=${cfg.kafkaBootstrap} topic=${cfg.kafkaTopic} bucket=${cfg.s3Bucket}")

    val spark = SparkSession
      .builder()
      .appName("spark-evolve")
      .getOrCreate()

    configureS3a(spark, cfg)

    val metrics = Metrics(spark.sparkContext)
    val registry = SchemaRegistry
      .fromDirectory(java.nio.file.Paths.get(cfg.schemasDir))
      .getOrElse(throw new RuntimeException(s"failed to load schemas from ${cfg.schemasDir}"))

    val versions = registry.versions(cfg.schemaSubject)
    val readerSchema = versions.lastOption
      .getOrElse(throw new RuntimeException(s"no schemas registered for subject ${cfg.schemaSubject}"))
    // All earlier versions are candidate writer schemas. The validator tries reader-only first
    // (no-evolution case), then walks back through these.
    val writerSchemas = versions.dropRight(1)

    val raw     = KafkaSource.read(spark, cfg.kafkaBootstrap, cfg.kafkaTopic)
    val totalIn = raw.count()
    metrics.recordsIn.add(totalIn)

    val split      = Validator.validate(raw, readerSchema, writerSchemas)
    val validCount = split.valid.count()
    val badCount   = split.bad.count()
    metrics.recordsValid.add(validCount)
    metrics.recordsBad.add(badCount)

    val agg = Aggregator.aggregate(split.valid, cfg.windowDuration)
    ParquetSink.writeAggregates(agg, cfg.outputPath)
    if (badCount > 0) {
      ParquetSink.writeBadRecords(split.bad, cfg.badPath)
    }
    // Optional Iceberg dual-write. Enable by setting SPARK_EVOLVE_ICEBERG_WAREHOUSE to a HadoopCatalog
    // root path (typically under MinIO/S3, e.g. `s3a://warehouse/iceberg`). The Iceberg table is named
    // `<catalog>.spark_evolve.agg_orders` and has the same partition layout as the Parquet sink.
    sys.env.get("SPARK_EVOLVE_ICEBERG_WAREHOUSE").foreach { warehouse =>
      log.info(s"iceberg dual-write enabled: warehouse=$warehouse")
      IcebergSink.configureCatalog(spark, warehouse)
      IcebergSink.writeAggregates(
        agg,
        IcebergSink.DefaultCatalogName,
        database = "spark_evolve",
        table = "agg_orders"
      )
    }

    log.info(s"metrics: ${metrics.snapshot}")
    spark.stop()
  }

  /** Structured-streaming entry point. Reuses the Validator + Aggregator topology on streaming Datasets via
    * `readStream` + `writeStream`. Watermark handling lives in the streaming aggregator so late events are
    * dropped after the configured lateness window. Trigger is processing-time on a 30s cadence.
    */
  private def runStreamingPipeline(): Unit = {
    val cfg = JobConfig.load()
    log.info(
      s"streaming: kafka=${cfg.kafkaBootstrap} topic=${cfg.kafkaTopic} bucket=${cfg.s3Bucket}"
    )

    val spark = SparkSession
      .builder()
      .appName("spark-evolve-streaming")
      .getOrCreate()

    configureS3a(spark, cfg)

    val registry = SchemaRegistry
      .fromDirectory(java.nio.file.Paths.get(cfg.schemasDir))
      .getOrElse(throw new RuntimeException(s"failed to load schemas from ${cfg.schemasDir}"))

    val versions = registry.versions(cfg.schemaSubject)
    val readerSchema = versions.lastOption
      .getOrElse(throw new RuntimeException(s"no schemas registered for subject ${cfg.schemaSubject}"))
    val writerSchemas = versions.dropRight(1)

    val raw   = KafkaStreamSource.readStream(spark, cfg.kafkaBootstrap, cfg.kafkaTopic)
    val split = StreamingValidator.validate(raw, readerSchema, writerSchemas)
    val aggDf = Aggregator.aggregateStreaming(split.valid, cfg.windowDuration)
    val ckpt  = sys.env.getOrElse("SPARK_EVOLVE_CHECKPOINT", "/tmp/spark-evolve-stream-checkpoint")
    val query = StreamingParquetSink.writeAggregates(aggDf, cfg.outputPath, ckpt)
    query.awaitTermination()
    spark.stop()
  }

  private def configureS3a(spark: SparkSession, cfg: JobConfig): Unit = {
    val hc = spark.sparkContext.hadoopConfiguration
    hc.set("fs.s3a.endpoint", cfg.s3Endpoint)
    hc.set("fs.s3a.access.key", cfg.s3AccessKey)
    hc.set("fs.s3a.secret.key", cfg.s3SecretKey)
    hc.setBoolean("fs.s3a.path.style.access", cfg.s3PathStyle)
    hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hc.set("fs.s3a.connection.ssl.enabled", if (cfg.s3Endpoint.startsWith("https")) "true" else "false")
  }
}
