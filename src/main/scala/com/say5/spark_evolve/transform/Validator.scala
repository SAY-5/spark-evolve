package com.say5.spark_evolve.transform

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/** Per-record Avro validation. Each Kafka `value` is a serialized record. We attempt to deserialize against
  * the *latest* registered schema. Records that deserialize cleanly land in `valid`. Records that fail
  * (truncated bytes, type mismatch, missing required field that has no default) land in `bad` with the
  * original payload and the failure reason.
  *
  * The split is intentional: ops can re-validate without re-reading from Kafka by writing the raw bytes
  * through this same step against an updated schema.
  */
object Validator {

  /** Output of [[validate]] — two DataFrames sharing the same input partitioning. */
  final case class Split(valid: DataFrame, bad: DataFrame)

  /** Schema for the Spark-side projection of a successfully decoded order. */
  val OrderRowSchema: StructType = StructType(
    Array(
      StructField("order_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false),
      StructField("customer_segment", StringType, nullable = false),
      StructField("total_cents", LongType, nullable = false),
      StructField("event_time", TimestampType, nullable = false),
      StructField("discount_cents", LongType, nullable = false)
    )
  )

  /** Schema for the bad-record sink. */
  val BadRowSchema: StructType = StructType(
    Array(
      StructField("topic", StringType, nullable = true),
      StructField("partition", IntegerType, nullable = true),
      StructField("offset", LongType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("payload", BinaryType, nullable = true),
      StructField("reason", StringType, nullable = false)
    )
  )

  /** Apply the split.
    *
    *   - `readerSchema` is what successful records will be promoted to.
    *   - `writerSchemas` is the ordered list of historical schemas events may have been written under (oldest
    *     first). The validator tries each in order until one decodes cleanly. The reader schema is also tried
    *     first — covers the no-evolution case.
    *
    * Fields the writer didn't have but that have defaults in the reader schema get filled in with those
    * defaults via Avro's standard reader-schema resolution.
    */
  def validate(kafkaDf: DataFrame, readerSchema: Schema, writerSchemas: Seq[Schema] = Nil): Split = {
    val readerJson  = readerSchema.toString
    val writerJsons = writerSchemas.map(_.toString).toVector
    val baseRdd     = kafkaDf.select("topic", "partition", "offset", "timestamp", "value").rdd.cache()

    val validRows = baseRdd.mapPartitions { iter =>
      val readerSchemaP = new Schema.Parser().parse(readerJson)
      val writers       = writerJsons.map(j => new Schema.Parser().parse(j))
      iter.flatMap { row =>
        val payload = row.getAs[Array[Byte]](4)
        decodeWithFallback(readerSchemaP, writers, payload).map { rec =>
          Row(
            str(rec, "order_id"),
            str(rec, "customer_id"),
            str(rec, "customer_segment"),
            lng(rec, "total_cents"),
            new java.sql.Timestamp(lng(rec, "event_time")),
            lngOr(rec, "discount_cents", 0L)
          )
        }
      }
    }

    val badRows = baseRdd.mapPartitions { iter =>
      val readerSchemaP = new Schema.Parser().parse(readerJson)
      val writers       = writerJsons.map(j => new Schema.Parser().parse(j))
      iter.flatMap { row =>
        val topic     = row.getString(0)
        val partition = row.getInt(1)
        val offset    = row.getLong(2)
        val ts        = row.getTimestamp(3)
        val payload   = row.getAs[Array[Byte]](4)
        decodeWithFallback(readerSchemaP, writers, payload) match {
          case Some(_) => None
          case None    => Some(Row(topic, partition, offset, ts, payload, "decode failed"))
        }
      }
    }

    val spark = kafkaDf.sparkSession
    Split(
      valid = spark.createDataFrame(validRows, OrderRowSchema),
      bad = spark.createDataFrame(badRows, BadRowSchema)
    )
  }

  /** Try the reader schema first (writer == reader), then each candidate writer schema in turn. Returns
    * Some(record) on first success.
    */
  private[transform] def decodeWithFallback(
    reader: Schema,
    writers: Seq[Schema],
    payload: Array[Byte]
  ): Option[GenericRecord] = {
    val singleReader = new GenericDatumReader[GenericRecord](reader)
    decode(singleReader, reader, payload).toOption.orElse {
      writers.iterator
        .flatMap { w =>
          val r = new GenericDatumReader[GenericRecord](w, reader)
          decode(r, reader, payload).toOption
        }
        .toSeq
        .headOption
    }
  }

  /** Convenience: count rows on each side without persisting. */
  def counts(split: Split): (Long, Long) = (split.valid.count(), split.bad.count())

  /** Default-value helper for the bad-record-only path. */
  def emitBadOnly(kafkaDf: DataFrame): DataFrame = {
    kafkaDf
      .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("value").as("payload"),
        lit("malformed").as("reason")
      )
  }

  // ---- helpers (package-private for tests) ----

  private[transform] def decode(
    reader: GenericDatumReader[GenericRecord],
    schema: Schema,
    payload: Array[Byte]
  ): Either[String, GenericRecord] = {
    if (payload == null || payload.isEmpty) {
      return Left("empty payload")
    }
    try {
      val decoder = DecoderFactory.get().binaryDecoder(payload, null)
      val rec     = reader.read(null, decoder)
      // Cheap structural sanity check: required fields present.
      val missing      = schema.getFields
      var i            = 0
      var miss: String = null
      while (i < missing.size() && miss == null) {
        val f = missing.get(i)
        if (rec.get(f.name) == null && !f.hasDefaultValue && !isNullable(f.schema())) {
          miss = f.name
        }
        i += 1
      }
      if (miss != null) Left(s"missing required field: $miss") else Right(rec)
    } catch {
      case t: Throwable => Left(s"${t.getClass.getSimpleName}: ${Option(t.getMessage).getOrElse("")}")
    }
  }

  private def str(rec: GenericRecord, name: String): String = {
    val v = rec.get(name)
    if (v == null) null else v.toString
  }

  private def lng(rec: GenericRecord, name: String): Long = {
    val v = rec.get(name)
    if (v == null) 0L else v.asInstanceOf[Number].longValue()
  }

  private def lngOr(rec: GenericRecord, name: String, default: Long): Long = {
    val v = rec.get(name)
    if (v == null) default else v.asInstanceOf[Number].longValue()
  }

  private def isNullable(s: Schema): Boolean = {
    import scala.jdk.CollectionConverters._
    s.getType == Schema.Type.UNION && s.getTypes.asScala.exists(_.getType == Schema.Type.NULL)
  }
}
