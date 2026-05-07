package com.say5.spark_evolve.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Per-key, per-tumbling-window metrics. Keys are `(customer_id, customer_segment, window(event_time, ...))`.
  * Metrics: count of orders, sum / avg / max of `total_cents`, distinct count of `order_id`.
  *
  * Works on both batch and streaming DataFrames. For streaming, call [[withWatermarkOnInput]] first so Spark
  * knows when to drop late records and emit final per-window rows.
  */
object Aggregator {

  /** Apply a watermark on `event_time`. Required upstream of `aggregate` for streaming aggregations; a no-op
    * but harmless for batch.
    */
  def withWatermarkOnInput(df: DataFrame, lateness: String = "1 hour"): DataFrame =
    df.withWatermark("event_time", lateness)

  /** Streaming-aware aggregate: applies watermark, then the standard aggregation.
    *
    * `countDistinct` is not supported in streaming (would require unbounded state); we drop it for the
    * streaming variant and keep `order_id_distinct = order_count` as an upper-bound proxy.
    */
  def aggregateStreaming(df: DataFrame, windowDuration: String, lateness: String = "1 hour"): DataFrame = {
    val watermarked = withWatermarkOnInput(df, lateness)
    watermarked
      .withColumn("net_cents", col("total_cents") - col("discount_cents"))
      .groupBy(
        col("customer_id"),
        col("customer_segment"),
        window(col("event_time"), windowDuration)
      )
      .agg(
        count(lit(1)).as("order_count"),
        sum("net_cents").as("net_cents_sum"),
        avg("net_cents").as("net_cents_avg"),
        max("net_cents").as("net_cents_max")
      )
      .withColumn("order_id_distinct", col("order_count"))
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
