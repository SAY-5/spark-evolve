package com.say5.spark_evolve.transform

import com.say5.spark_evolve.schema.SchemaRegistry
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream

/** Unit-level tests for [[StreamingValidator]]. We exercise it on a *batch* DataFrame here — the validator is
  * shaped to work on streaming Datasets too, but the same lineage is testable without bringing up Kafka. The
  * end-to-end streaming path is covered by `StreamingIT`.
  */
class StreamingValidatorSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("streaming-validator-spec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private lazy val readerSchema: Schema = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get

  private def encode(schema: Schema, fill: GenericRecord => Unit): Array[Byte] = {
    val r = new GenericData.Record(schema)
    fill(r)
    val w   = new GenericDatumWriter[GenericRecord](schema)
    val out = new ByteArrayOutputStream(256)
    val enc = EncoderFactory.get().binaryEncoder(out, null)
    w.write(r, enc); enc.flush()
    out.toByteArray
  }

  private def kafkaLikeRows(payloads: Seq[Array[Byte]]) = {
    val schema = StructType(
      Array(
        StructField("topic", StringType, nullable = false),
        StructField("partition", IntegerType, nullable = false),
        StructField("offset", LongType, nullable = false),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("value", BinaryType, nullable = false)
      )
    )
    val rows = payloads.zipWithIndex.map { case (p, i) =>
      org.apache.spark.sql.Row(
        "orders",
        0,
        i.toLong,
        new java.sql.Timestamp(java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli + i * 1000L),
        p
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  test("StreamingValidator splits valid v2 records from random garbage") {
    val baseTs = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli
    val good = (0 until 10).map { i =>
      encode(
        readerSchema,
        r => {
          r.put("order_id", f"order-$i%05d")
          r.put("customer_id", f"customer-$i%03d")
          r.put("customer_segment", "retail")
          r.put("total_cents", 1000L + i)
          r.put("event_time", baseTs + i * 60_000L)
          r.put("discount_cents", 0L)
        }
      )
    }
    // Pick payloads that the Avro decoder can't parse without throwing — leading byte 0xff indicates
    // a negative zigzag length which the string decoder rejects. Some random byte patterns *do*
    // happen to decode to a record with all-null fields (this is what the property-based fuzz test
    // documents); we want unambiguous decode-failure cases here.
    val bad = Seq(
      Array.fill[Byte](16)(0xff.toByte),
      Array.fill[Byte](24)(0xfe.toByte)
    )
    val rows  = kafkaLikeRows(good ++ bad)
    val split = StreamingValidator.validate(rows, readerSchema, Seq.empty)
    split.valid.count() should be >= 10L
    split.bad.count() should be >= 1L
    val firstBad = split.bad.first()
    firstBad.getAs[String]("reason") should include("decode failed")
  }

  test("StreamingValidator falls back to writer schemas for v1 payloads") {
    val v1     = SchemaRegistry.parseFile("schemas/orders/v1.avsc").get
    val v2     = SchemaRegistry.parseFile("schemas/orders/v2.avsc").get
    val baseTs = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli
    val v1Encoded = (0 until 5).map { i =>
      encode(
        v1,
        r => {
          r.put("order_id", f"order-$i%05d")
          r.put("customer_id", f"customer-$i%03d")
          r.put("customer_segment", "retail")
          r.put("total_cents", 1000L + i)
          r.put("event_time", baseTs + i * 60_000L)
        }
      )
    }
    val rows  = kafkaLikeRows(v1Encoded)
    val split = StreamingValidator.validate(rows, readerSchema = v2, writerSchemas = Seq(v1))
    split.valid.count() shouldBe 5L
    split.bad.count() shouldBe 0L
    // discount_cents must default to 0 for v1 records read with v2 reader.
    split.valid
      .select("discount_cents")
      .distinct()
      .collect()
      .map(_.getLong(0))
      .toSet shouldBe Set(0L)
  }
}
