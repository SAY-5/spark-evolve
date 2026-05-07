package com.say5.spark_evolve.obs

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/** Accumulator-based counters. Cheap; visible in the Spark UI under "Stages". The pipeline records a small
  * handful of operational signals — bytes read, good/bad record counts, and per-key cardinality — for ops
  * dashboards.
  */
final class Metrics(sc: SparkContext) {
  val recordsIn: LongAccumulator    = sc.longAccumulator("records_in")
  val recordsValid: LongAccumulator = sc.longAccumulator("records_valid")
  val recordsBad: LongAccumulator   = sc.longAccumulator("records_bad")
  val bytesIn: LongAccumulator      = sc.longAccumulator("bytes_in")

  def snapshot: Map[String, Long] = Map(
    "records_in"    -> recordsIn.value,
    "records_valid" -> recordsValid.value,
    "records_bad"   -> recordsBad.value,
    "bytes_in"      -> bytesIn.value
  )
}

object Metrics {
  def apply(sc: SparkContext): Metrics = new Metrics(sc)
}
