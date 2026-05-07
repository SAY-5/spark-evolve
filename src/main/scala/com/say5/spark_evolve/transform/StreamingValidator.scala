package com.say5.spark_evolve.transform

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}

/** Streaming-safe validator. The batch [[Validator]] uses `kafkaDf.rdd` and `count()` which are not permitted
  * on a streaming DataFrame. This variant uses only DataFrame transformations + a typed `mapPartitions`,
  * which keeps the lineage compatible with a `writeStream` sink.
  *
  * Internally each input row is mapped to a [[StreamingValidator.Tagged]] case class carrying a `valid`
  * boolean plus all candidate fields; downstream filters split valid from bad without crossing into the RDD
  * API.
  */
object StreamingValidator {

  /** Output of [[validate]] — two streaming DataFrames sharing the same input schema. */
  final case class Split(valid: DataFrame, bad: DataFrame)

  /** Wide row carrying both the decoded order fields and the bad-record metadata. Either branch will be
    * null-filled depending on whether decode succeeded.
    */
  final case class Tagged(
    valid: Boolean,
    orderId: String,
    customerId: String,
    customerSegment: String,
    totalCents: java.lang.Long,
    eventTime: java.sql.Timestamp,
    discountCents: java.lang.Long,
    topic: String,
    partition: java.lang.Integer,
    offset: java.lang.Long,
    timestamp: java.sql.Timestamp,
    payload: Array[Byte],
    reason: String
  )

  /** Build the streaming-safe split.
    *
    *   - input must contain Kafka columns `topic`, `partition`, `offset`, `timestamp`, `value`.
    *   - successful records are projected to the same shape `Validator.OrderRowSchema` produces.
    *   - failures are projected to the same shape `Validator.BadRowSchema` produces.
    */
  def validate(kafkaDf: DataFrame, readerSchema: Schema, writerSchemas: Seq[Schema] = Nil): Split = {
    val readerJson  = readerSchema.toString
    val writerJsons = writerSchemas.map(_.toString).toVector

    implicit val taggedEnc: Encoder[Tagged] = Encoders.product[Tagged]
    implicit val tupleEnc: Encoder[(String, Int, Long, java.sql.Timestamp, Array[Byte])] =
      Encoders.tuple(
        Encoders.STRING,
        Encoders.scalaInt,
        Encoders.scalaLong,
        Encoders.TIMESTAMP,
        Encoders.BINARY
      )

    val tagged = kafkaDf
      .select(col("topic"), col("partition"), col("offset"), col("timestamp"), col("value"))
      .as[(String, Int, Long, java.sql.Timestamp, Array[Byte])]
      .mapPartitions { iter =>
        val readerS = new Schema.Parser().parse(readerJson)
        val writers = writerJsons.map(j => new Schema.Parser().parse(j))
        iter.map { case (topic, partition, offset, ts, payload) =>
          decodeWithFallback(readerS, writers, payload) match {
            case Some(rec) =>
              Tagged(
                valid = true,
                orderId = str(rec, "order_id"),
                customerId = str(rec, "customer_id"),
                customerSegment = str(rec, "customer_segment"),
                totalCents = lng(rec, "total_cents"),
                eventTime = new java.sql.Timestamp(lng(rec, "event_time")),
                discountCents = lngOr(rec, "discount_cents", 0L),
                topic = null,
                partition = null,
                offset = null,
                timestamp = null,
                payload = null,
                reason = null
              )
            case None =>
              Tagged(
                valid = false,
                orderId = null,
                customerId = null,
                customerSegment = null,
                totalCents = null,
                eventTime = null,
                discountCents = null,
                topic = topic,
                partition = Integer.valueOf(partition),
                offset = java.lang.Long.valueOf(offset),
                timestamp = ts,
                payload = payload,
                reason = "decode failed"
              )
          }
        }
      }
      .toDF()

    val valid = tagged
      .filter(col("valid") === true)
      .select(
        col("orderId").as("order_id"),
        col("customerId").as("customer_id"),
        col("customerSegment").as("customer_segment"),
        col("totalCents").as("total_cents"),
        col("eventTime").as("event_time"),
        col("discountCents").as("discount_cents")
      )

    val bad = tagged
      .filter(col("valid") === false)
      .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("payload"),
        col("reason")
      )

    Split(valid = valid, bad = bad)
  }

  // ---- Helpers ----

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

  private def decode(
    reader: GenericDatumReader[GenericRecord],
    schema: Schema,
    payload: Array[Byte]
  ): Either[String, GenericRecord] = {
    if (payload == null || payload.isEmpty) {
      return Left("empty payload")
    }
    try {
      val decoder      = DecoderFactory.get().binaryDecoder(payload, null)
      val rec          = reader.read(null, decoder)
      val fields       = schema.getFields
      var i            = 0
      var miss: String = null
      while (i < fields.size() && miss == null) {
        val f = fields.get(i)
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

  private def lng(rec: GenericRecord, name: String): java.lang.Long = {
    val v = rec.get(name)
    if (v == null) java.lang.Long.valueOf(0L) else java.lang.Long.valueOf(v.asInstanceOf[Number].longValue())
  }

  private def lngOr(rec: GenericRecord, name: String, default: Long): java.lang.Long = {
    val v = rec.get(name)
    if (v == null) java.lang.Long.valueOf(default)
    else java.lang.Long.valueOf(v.asInstanceOf[Number].longValue())
  }

  private def isNullable(s: Schema): Boolean = {
    import scala.jdk.CollectionConverters._
    s.getType == Schema.Type.UNION && s.getTypes.asScala.exists(_.getType == Schema.Type.NULL)
  }
}
