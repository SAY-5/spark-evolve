package com.say5.spark_evolve.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Structured-streaming counterpart to [[KafkaSource]]. Returns a streaming DataFrame backed by Kafka.
  *
  * The schema is identical to the batch source (key, value, topic, partition, offset, timestamp,
  * timestampType) which is what makes the same Validator + Aggregator topology reusable across batch and
  * streaming entry points.
  */
object KafkaStreamSource {

  /** Read a Kafka topic as a streaming DataFrame.
    *
    * @param spark
    *   Spark session
    * @param bootstrap
    *   Kafka bootstrap servers
    * @param topic
    *   topic to consume
    * @param startingOffsets
    *   offset spec, e.g. "earliest" or "latest"
    */
  def readStream(
    spark: SparkSession,
    bootstrap: String,
    topic: String,
    startingOffsets: String = "latest"
  ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .load()
  }
}
