package com.say5.spark_evolve.transform

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Iceberg counterpart to [[ParquetSink]].
  *
  * Writes the aggregated dataset to an Apache Iceberg table backed by HadoopCatalog (a file-based catalog
  * with no external metastore). The catalog warehouse lives at `<warehousePath>` — typically a path under
  * MinIO or S3 — and table data is written in the same `(date_hour, customer_segment)` partition layout the
  * Parquet sink uses, so the two sinks can be queried side by side.
  *
  * Iceberg adds three things over plain Parquet:
  *
  *   - **ACID writes**: the table snapshot is updated atomically. Concurrent writers don't see each other's
  *     half-written files.
  *   - **Time travel**: every commit produces a snapshot ID; queries can pin to an earlier snapshot.
  *   - **Schema evolution**: in-place add/rename/drop column without rewriting historical data files.
  *
  * The trade-off is one extra metadata layer (manifest + manifest-list + snapshot files in
  * `<warehouse>/<db>/<table>/metadata/`) which is overhead for small tables but pays off the first time you
  * need any of the three above.
  */
object IcebergSink {

  /** Name of the catalog as Spark sees it. The pipeline also supports overriding it via env so tests can use
    * a transient catalog name without polluting the operator's namespace.
    */
  val DefaultCatalogName: String = "spark_evolve_iceberg"

  /** Configure a Spark session to use a Hadoop-backed Iceberg catalog rooted at `warehousePath`. Idempotent —
    * calling it twice with the same args is a no-op.
    */
  def configureCatalog(
    spark: SparkSession,
    warehousePath: String,
    catalogName: String = DefaultCatalogName
  ): Unit = {
    val cfg = spark.conf
    cfg.set(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
    cfg.set(s"spark.sql.catalog.$catalogName.type", "hadoop")
    cfg.set(s"spark.sql.catalog.$catalogName.warehouse", warehousePath)
    val existing = cfg.getOption("spark.sql.extensions").getOrElse("")
    val needed   = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    if (!existing.split(",").map(_.trim).toSet.contains(needed)) {
      val merged = if (existing.isEmpty) needed else s"$existing,$needed"
      cfg.set("spark.sql.extensions", merged)
    }
  }

  /** Write `df` to an Iceberg table named `<catalog>.<db>.<table>`. Creates the table on the first call
    * (partitioned by `(date_hour, customer_segment)`); appends on subsequent calls.
    */
  def writeAggregates(
    df: DataFrame,
    catalogName: String,
    database: String,
    table: String
  ): Unit = {
    val spark = df.sparkSession
    val fqn   = s"$catalogName.$database.$table"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.$database")
    if (!tableExists(spark, catalogName, database, table)) {
      df.writeTo(fqn)
        .partitionedBy(df("date_hour"), df("customer_segment"))
        .using("iceberg")
        .createOrReplace()
    } else {
      df.write
        .format("iceberg")
        .mode(SaveMode.Append)
        .save(fqn)
    }
  }

  private def tableExists(spark: SparkSession, catalog: String, db: String, table: String): Boolean = {
    try {
      spark.sql(s"DESCRIBE TABLE $catalog.$db.$table").collect()
      true
    } catch {
      case _: Throwable => false
    }
  }
}
