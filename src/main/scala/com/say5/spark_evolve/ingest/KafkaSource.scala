package com.say5.spark_evolve.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Thin wrapper that reads a snapshot from a Kafka topic as a Spark batch DataFrame. The pipeline is
  * intentionally batch-oriented; the structured streaming swap-path is a one-line change to `readStream`.
  *
  * The returned DataFrame has Kafka's standard columns: key, value, topic, partition, offset, timestamp,
  * timestampType. Downstream code is responsible for decoding `value` (Avro bytes) against the registered
  * schema.
  */
object KafkaSource {

  /** Read a Kafka topic as a DataFrame.
    *
    * @param spark
    *   Spark session
    * @param bootstrap
    *   Kafka bootstrap servers
    * @param topic
    *   topic to consume
    * @param startingOffsets
    *   offset spec, e.g. "earliest" or "latest"
    * @param endingOffsets
    *   offset spec for batch reads, e.g. "latest"
    */
  def read(
    spark: SparkSession,
    bootstrap: String,
    topic: String,
    startingOffsets: String = "earliest",
    endingOffsets: String = "latest"
  ): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", endingOffsets)
      .option("failOnDataLoss", "false")
      .load()
  }
}
