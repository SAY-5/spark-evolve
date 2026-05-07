package com.say5.spark_evolve.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

/** Streaming counterpart to [[ParquetSink]].
  *
  * Writes a streaming dataset to Parquet on the same `date_hour` / `customer_segment` partition layout the
  * batch sink uses, so downstream consumers see one logical table regardless of which entry point produced
  * the data.
  *
  * Uses `OutputMode.Append`, requires a checkpoint directory, and picks a processing-time trigger so files
  * land on a predictable cadence rather than per micro-batch.
  */
object StreamingParquetSink {

  /** Start the streaming query. Caller is responsible for `awaitTermination()` and for keeping the
    * checkpointPath stable across restarts.
    *
    * @param df
    *   pre-aggregated streaming DataFrame (use [[Aggregator]] upstream)
    * @param outputPath
    *   base path for Parquet output
    * @param checkpointPath
    *   Spark structured-streaming checkpoint location (must persist across restarts)
    * @param triggerInterval
    *   processing-time trigger, e.g. "30 seconds"
    * @param queryName
    *   Spark UI / metrics-friendly query name
    */
  def writeAggregates(
    df: DataFrame,
    outputPath: String,
    checkpointPath: String,
    triggerInterval: String = "30 seconds",
    queryName: String = "spark-evolve-streaming-agg"
  ): StreamingQuery = {
    df.writeStream
      .format("parquet")
      .outputMode("append")
      .partitionBy("date_hour", "customer_segment")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .queryName(queryName)
      .start()
  }
}
