package com.say5.spark_evolve.config

import com.typesafe.config.{Config => TConfig, ConfigFactory}

/** Job configuration loaded from `application.conf` or env-overridden for tests.
  *
  * Example minimal `application.conf`:
  * {{{
  *   spark-evolve {
  *     kafka.bootstrap = "localhost:9092"
  *     kafka.topic     = "orders"
  *     kafka.bad-topic = "orders-bad"
  *     s3 {
  *       endpoint   = "http://localhost:9000"
  *       bucket     = "warehouse"
  *       output-prefix = "orders_hourly"
  *       bad-prefix    = "bad_records"
  *       access-key = "minioadmin"
  *       secret-key = "minioadmin"
  *       path-style = true
  *     }
  *     schemas {
  *       dir     = "schemas"
  *       subject = "orders"
  *       compatibility = "backward"
  *     }
  *     window {
  *       duration = "1 hour"
  *     }
  *   }
  * }}}
  */
final case class JobConfig(
  kafkaBootstrap: String,
  kafkaTopic: String,
  kafkaBadTopic: String,
  s3Endpoint: String,
  s3Bucket: String,
  s3OutputPrefix: String,
  s3BadPrefix: String,
  s3AccessKey: String,
  s3SecretKey: String,
  s3PathStyle: Boolean,
  schemasDir: String,
  schemaSubject: String,
  compatibility: String,
  windowDuration: String
) {
  def outputPath: String = s"s3a://$s3Bucket/$s3OutputPrefix"
  def badPath: String    = s"s3a://$s3Bucket/$s3BadPrefix"
}

object JobConfig {

  def load(): JobConfig = fromTypesafe(ConfigFactory.load())

  def fromTypesafe(c: TConfig): JobConfig = {
    val r = c.getConfig("spark-evolve")
    JobConfig(
      kafkaBootstrap = r.getString("kafka.bootstrap"),
      kafkaTopic = r.getString("kafka.topic"),
      kafkaBadTopic = r.getString("kafka.bad-topic"),
      s3Endpoint = r.getString("s3.endpoint"),
      s3Bucket = r.getString("s3.bucket"),
      s3OutputPrefix = r.getString("s3.output-prefix"),
      s3BadPrefix = r.getString("s3.bad-prefix"),
      s3AccessKey = r.getString("s3.access-key"),
      s3SecretKey = r.getString("s3.secret-key"),
      s3PathStyle = r.getBoolean("s3.path-style"),
      schemasDir = r.getString("schemas.dir"),
      schemaSubject = r.getString("schemas.subject"),
      compatibility = r.getString("schemas.compatibility"),
      windowDuration = r.getString("window.duration")
    )
  }
}
