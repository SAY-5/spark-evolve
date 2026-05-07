package com.say5.spark_evolve.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Per-key, per-tumbling-window metrics. Keys are `(customer_id, customer_segment, window(event_time, ...))`.
  * Metrics: count of orders, sum / avg / max of `total_cents`, distinct count of `order_id`.
  */
object Aggregator {

  /** @param df
    *   validated orders
    * @param windowDuration
    *   Spark window spec (e.g. "1 hour")
    */
  def aggregate(df: DataFrame, windowDuration: String): DataFrame = {
    df.withColumn("net_cents", col("total_cents") - col("discount_cents"))
      .groupBy(
        col("customer_id"),
        col("customer_segment"),
        window(col("event_time"), windowDuration)
      )
      .agg(
        count(lit(1)).as("order_count"),
        sum("net_cents").as("net_cents_sum"),
        avg("net_cents").as("net_cents_avg"),
        max("net_cents").as("net_cents_max"),
        countDistinct("order_id").as("order_id_distinct")
      )
      .select(
        col("customer_id"),
        col("customer_segment"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        date_format(col("window.start"), "yyyy-MM-dd-HH").as("date_hour"),
        col("order_count"),
        col("net_cents_sum"),
        col("net_cents_avg"),
        col("net_cents_max"),
        col("order_id_distinct")
      )
  }
}
