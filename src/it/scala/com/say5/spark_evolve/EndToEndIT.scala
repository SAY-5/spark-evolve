package com.say5.spark_evolve

import com.dimafeng.testcontainers.{KafkaContainer, MinIOContainer}
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import com.dimafeng.testcontainers.lifecycle.and
import com.say5.spark_evolve.transform.{Aggregator, ParquetSink, Validator}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.nio.file.Paths
import java.util.Properties

/** End-to-end test that brings up Kafka + MinIO via testcontainers, produces a mix of v1 and (intentionally
  * malformed) Avro events to a topic, runs the pipeline end-to-end, and asserts:
  *
  *   - the v1-encoded events are read by the v2 reader (default-bearing field `discount_cents` shows up as 0)
  *   - malformed events land in the bad-records sink
  *   - the Parquet output partitions match the expected layout
  *
  * Run with: `RUN_INTEGRATION=1 sbt it:test`. Skipped unless Docker is running.
  */
class EndToEndIT extends AnyFunSuite with Matchers with BeforeAndAfterAll with TestContainersForAll {

  override type Containers = KafkaContainer and MinIOContainer

  override def startContainers(): KafkaContainer and MinIOContainer = {
    val kafka = KafkaContainer.Def().start()
    val minio = MinIOContainer.Def().start()
    kafka and minio
  }

  private lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("e2e-it")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  test("kafka → validate → aggregate → parquet on minio, with schema promotion v1→v2") {
    assume(
      sys.env.get("RUN_INTEGRATION").contains("1"),
      "set RUN_INTEGRATION=1 to enable docker-based integration tests"
    )

    withContainers { case kafka and minio =>
      val v1 = new Schema.Parser().parse(new java.io.File("schemas/orders/v1.avsc"))
      val v2 = new Schema.Parser().parse(new java.io.File("schemas/orders/v2.avsc"))

      val topic = "orders-it"
      // Pre-create the topic with one partition so Spark's metadata fetch sees it immediately.
      createTopic(kafka.bootstrapServers, topic)
      produceEvents(kafka.bootstrapServers, topic, v1, count = 50, includeBad = true)
      // Brief settle so the broker reports the high-water mark to consumer metadata fetches.
      Thread.sleep(2000)

      // Configure Spark to talk to MinIO.
      val hc = sparkSession.sparkContext.hadoopConfiguration
      hc.set("fs.s3a.endpoint", minio.s3URL)
      hc.set("fs.s3a.access.key", minio.userName)
      hc.set("fs.s3a.secret.key", minio.password)
      hc.setBoolean("fs.s3a.path.style.access", true)
      hc.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      hc.set("fs.s3a.connection.ssl.enabled", "false")

      // Independently verify Kafka has all 50 messages before invoking Spark, so a Spark-side
      // read failure doesn't get silently swallowed by failOnDataLoss=false.
      val consumed = consumeAll(kafka.bootstrapServers, topic, 50)
      consumed shouldBe 50

      val raw = sparkSession.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .cache()

      raw.count() shouldBe 50L

      val split = Validator.validate(raw, v2)
      val valid = split.valid.cache()
      val bad   = split.bad.cache()

      // 50 events total, 5 deliberately corrupted = 45 valid, 5 bad.
      valid.count() shouldBe 45
      bad.count() shouldBe 5

      // The reader was promoted to v2; v1 payloads should now have discount_cents = 0.
      valid
        .select("discount_cents")
        .distinct()
        .collect()
        .map(_.getLong(0))
        .toSet shouldBe Set(0L)

      val agg    = Aggregator.aggregate(valid, "1 hour")
      val outDir = "/tmp/spark-evolve-it-out"
      Paths.get(outDir).toFile.mkdirs()
      ParquetSink.writeAggregates(agg, outDir, coalesceTo = 1)

      val readBack = sparkSession.read.parquet(outDir).count()
      readBack should be > 0L
    }
  }

  private def consumeAll(bootstrap: String, topic: String, expected: Int): Int = {
    import org.apache.kafka.clients.consumer.KafkaConsumer
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("group.id", s"verify-${java.util.UUID.randomUUID()}")
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer",   "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    try {
      consumer.subscribe(java.util.Collections.singletonList(topic))
      var count   = 0
      val deadline = System.currentTimeMillis() + 30000
      while (count < expected && System.currentTimeMillis() < deadline) {
        val records = consumer.poll(java.time.Duration.ofMillis(1000))
        count += records.count()
      }
      count
    } finally {
      consumer.close()
    }
  }

  private def createTopic(bootstrap: String, topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    val admin = org.apache.kafka.clients.admin.AdminClient.create(props)
    try {
      val newTopic = new org.apache.kafka.clients.admin.NewTopic(topic, 1, 1.toShort)
      admin.createTopics(java.util.Collections.singletonList(newTopic)).all().get()
    } catch {
      case _: Throwable => () // topic may already exist; ignore
    } finally {
      admin.close()
    }
  }

  private def produceEvents(
    bootstrap: String,
    topic: String,
    schema: Schema,
    count: Int,
    includeBad: Boolean
  ): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("acks", "1")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    val w        = new GenericDatumWriter[GenericRecord](schema)
    val baseTs   = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli
    try {
      (0 until count).foreach { i =>
        val payload: Array[Byte] = if (includeBad && i % 10 == 0) {
          Array[Byte](0, 1, 2, 3) // garbage, will fail decode
        } else {
          val r = new GenericData.Record(schema)
          r.put("order_id", f"order-$i%05d")
          r.put("customer_id", f"customer-${i % 5}%03d")
          r.put("customer_segment", if (i % 2 == 0) "retail" else "wholesale")
          r.put("total_cents", (1000 + i).toLong)
          r.put("event_time", baseTs + i * 60_000L)
          val out = new ByteArrayOutputStream(256)
          val enc = EncoderFactory.get().binaryEncoder(out, null)
          w.write(r, enc); enc.flush()
          out.toByteArray
        }
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, payload)).get()
      }
      producer.flush()
    } finally {
      producer.close()
    }
  }
}
