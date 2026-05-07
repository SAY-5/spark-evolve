package com.say5.spark_evolve.transform

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/** Roundtrip test for [[IcebergSink]] backed by a HadoopCatalog rooted at a temp directory.
  *
  * Writes a synthetic aggregate DataFrame, reads it back via Spark's table API, and asserts row-count and
  * sample-field equality. This exercises the Iceberg metadata layer (manifest writes, snapshot rotation)
  * without bringing up a real metastore.
  */
class IcebergSinkSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private val warehousePath = Files.createTempDirectory("spark-evolve-iceberg-warehouse").toAbsolutePath

  private lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .appName("iceberg-sink-spec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )
      .config("spark.sql.catalog.test_iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.test_iceberg.type", "hadoop")
      .config("spark.sql.catalog.test_iceberg.warehouse", warehousePath.toString)
      .getOrCreate()
    s
  }

  override def afterAll(): Unit = {
    try spark.stop()
    catch { case _: Throwable => () }
    // Best-effort cleanup of the warehouse dir.
    try {
      val pb = new ProcessBuilder("rm", "-rf", warehousePath.toString)
      pb.start().waitFor()
    } catch { case _: Throwable => () }
  }

  private val aggSchema = StructType(
    Array(
      StructField("customer_id", StringType, nullable = false),
      StructField("customer_segment", StringType, nullable = false),
      StructField("window_start", TimestampType, nullable = false),
      StructField("window_end", TimestampType, nullable = false),
      StructField("date_hour", StringType, nullable = false),
      StructField("order_count", LongType, nullable = false),
      StructField("net_cents_sum", LongType, nullable = false),
      StructField("net_cents_avg", DoubleType, nullable = false),
      StructField("net_cents_max", LongType, nullable = false),
      StructField("order_id_distinct", LongType, nullable = false)
    )
  )

  private def sampleAggregates(n: Int) = {
    val baseTs = java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli
    val rows = (0 until n).map { i =>
      val hour = i % 5
      Row(
        f"customer-$i%05d",
        if (i % 2 == 0) "retail" else "wholesale",
        new java.sql.Timestamp(baseTs + hour * 3600_000L),
        new java.sql.Timestamp(baseTs + (hour + 1) * 3600_000L),
        f"2026-01-01-$hour%02d",
        (i + 1).toLong,
        (1000L * (i + 1)),
        1000.0 * (i + 1),
        1500L * (i + 1),
        (i + 1).toLong
      )
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), aggSchema)
  }

  test("IcebergSink roundtrip: write → read back → row count + sample fields match") {
    val n  = 50
    val df = sampleAggregates(n)

    IcebergSink.writeAggregates(df, "test_iceberg", "spark_evolve_test", "agg_orders_v1")

    val readBack = spark.read
      .format("iceberg")
      .load("test_iceberg.spark_evolve_test.agg_orders_v1")

    readBack.count() shouldBe n.toLong

    // Sample-field check: pick any row and confirm a couple of columns roundtripped intact.
    val sample = readBack.orderBy("customer_id").limit(1).collect().head
    sample.getAs[String]("customer_id") shouldBe "customer-00000"
    sample.getAs[Long]("order_count") shouldBe 1L
    sample.getAs[Long]("net_cents_sum") shouldBe 1000L

    // Distinct date_hour values should be exactly the 5 hour buckets we generated.
    val hours = readBack.select("date_hour").distinct().collect().map(_.getString(0)).toSet
    hours shouldBe Set("2026-01-01-00", "2026-01-01-01", "2026-01-01-02", "2026-01-01-03", "2026-01-01-04")
  }

  test("IcebergSink supports append on a second write") {
    val first  = sampleAggregates(10)
    val second = sampleAggregates(5)
    IcebergSink.writeAggregates(first, "test_iceberg", "spark_evolve_test", "agg_orders_append")
    IcebergSink.writeAggregates(second, "test_iceberg", "spark_evolve_test", "agg_orders_append")
    val readBack = spark.read
      .format("iceberg")
      .load("test_iceberg.spark_evolve_test.agg_orders_append")
    readBack.count() shouldBe 15L
  }
}
