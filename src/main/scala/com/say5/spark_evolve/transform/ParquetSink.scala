package com.say5.spark_evolve.transform

import org.apache.spark.sql.{DataFrame, SaveMode}

/** Partitioned Parquet writer. Data lands at:
  * `<base>/date_hour=YYYY-MM-DD-HH/customer_segment=<seg>/part-*.parquet`
  *
  * Re-runs use `mode=append`. Idempotency for re-processing the same input window is the consumer's
  * responsibility — see ARCHITECTURE.md for the dedup contract.
  */
object ParquetSink {

  def writeAggregates(df: DataFrame, basePath: String, coalesceTo: Int = 4): Unit = {
    df.coalesce(math.max(1, coalesceTo))
      .write
      .mode(SaveMode.Append)
      .partitionBy("date_hour", "customer_segment")
      .parquet(basePath)
  }

  def writeBadRecords(df: DataFrame, basePath: String): Unit = {
    df.write
      .mode(SaveMode.Append)
      .parquet(basePath)
  }
}
