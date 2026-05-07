package com.say5.spark_evolve.transform

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class AggregatorSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  // Lazy val makes this a stable identifier so `import sparkSession.implicits._` compiles.
  private lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("aggregator-spec")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  override def beforeAll(): Unit = {
    sparkSession.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
  }

  private def ts(s: String): Timestamp = Timestamp.valueOf(s)

  test("aggregate produces one row per (customer, segment, hour) with correct count") {
    import sparkSession.implicits._
    val df = Seq(
      ("o1", "c1", "S", 1000L, ts("2026-01-01 10:00:00"), 0L),
      ("o2", "c1", "S", 2000L, ts("2026-01-01 10:30:00"), 100L),
      ("o3", "c1", "S", 500L, ts("2026-01-01 11:00:00"), 0L),
      ("o4", "c2", "P", 700L, ts("2026-01-01 10:00:00"), 0L)
    ).toDF("order_id", "customer_id", "customer_segment", "total_cents", "event_time", "discount_cents")

    val agg = Aggregator.aggregate(df, "1 hour").collect()
    agg should have length 3
    val c1at10 = agg.find(r => r.getString(0) == "c1" && r.getString(4) == "2026-01-01-10").get
    c1at10.getAs[Long]("order_count") shouldBe 2
    c1at10.getAs[Long]("net_cents_sum") shouldBe (1000L + 1900L)
  }

  test("distinct count handles duplicate order_ids in the same window") {
    import sparkSession.implicits._
    val df = Seq(
      ("o1", "c1", "S", 1000L, ts("2026-01-01 10:00:00"), 0L),
      ("o1", "c1", "S", 1000L, ts("2026-01-01 10:30:00"), 0L),
      ("o2", "c1", "S", 1000L, ts("2026-01-01 10:45:00"), 0L)
    ).toDF("order_id", "customer_id", "customer_segment", "total_cents", "event_time", "discount_cents")

    val agg = Aggregator.aggregate(df, "1 hour").collect()
    agg should have length 1
    agg.head.getAs[Long]("order_id_distinct") shouldBe 2
    agg.head.getAs[Long]("order_count") shouldBe 3
  }

  test("max/avg are computed on net (total - discount)") {
    import sparkSession.implicits._
    val df = Seq(
      ("o1", "c1", "S", 1000L, ts("2026-01-01 10:00:00"), 100L),
      ("o2", "c1", "S", 2000L, ts("2026-01-01 10:30:00"), 500L)
    ).toDF("order_id", "customer_id", "customer_segment", "total_cents", "event_time", "discount_cents")

    val agg = Aggregator.aggregate(df, "1 hour").collect()
    agg should have length 1
    val r = agg.head
    r.getAs[Long]("net_cents_max") shouldBe 1500L
    r.getAs[Double]("net_cents_avg") shouldBe ((900.0 + 1500.0) / 2)
  }
}
