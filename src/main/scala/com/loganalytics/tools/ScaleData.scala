package com.loganalytics.tools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ScaleData {
  final case class Config(
      logsPath: String = "data/logs.parquet",
      deploymentsPath: String = "data/deployments.parquet",
      hostMetaPath: String = "data/host_meta.parquet",
      outputRoot: String = ".",
      targetRows: Seq[Long] = Seq(2000000L, 5000000L, 10000000L)
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    val spark = SparkSession.builder()
      .appName("ScaleLogAnalyticsData")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val logs = spark.read.parquet(config.logsPath).cache()
    val deployments = spark.read.parquet(config.deploymentsPath).cache()
    val hostMeta = spark.read.parquet(config.hostMetaPath).cache()

    val baseCount = logs.count()
    require(baseCount > 0, "Source logs dataset is empty; cannot scale data.")

    config.targetRows.foreach { target =>
      val dirName = s"data_${target}"
      val outputBase = s"${config.outputRoot.stripSuffix("/")}/$dirName"
      println(s"[SCALE] Generating dataset for targetRows=$target under $outputBase")

      val scaledLogs = expandLogs(spark, logs, baseCount, target)
      scaledLogs.write.mode("overwrite").parquet(s"$outputBase/logs")

      deployments.write.mode("overwrite").parquet(s"$outputBase/deployments")
      hostMeta.write.mode("overwrite").parquet(s"$outputBase/host_meta")
    }

    logs.unpersist()
    deployments.unpersist()
    hostMeta.unpersist()
    spark.stop()
  }

  private def expandLogs(spark: SparkSession, logs: DataFrame, baseCount: Long, targetRows: Long): DataFrame = {
    val copies = math.ceil(targetRows.toDouble / baseCount.toDouble).toLong
    val duplicateIds = spark.range(copies).toDF("duplicate_id")

    logs.crossJoin(duplicateIds)
      .withColumn("row_num", row_number().over(Window.orderBy(monotonically_increasing_id())))
      .filter(col("row_num") <= lit(targetRows))
      .drop("row_num")
      .withColumn(
        "timestamp",
        to_timestamp(from_unixtime(unix_timestamp(col("timestamp")) + (col("duplicate_id") * lit(3600L))))
      )
      .withColumn(
        "user_id",
        when(col("user_id").isNull, lit(null).cast("string"))
          .otherwise(concat(col("user_id"), lit("_copy_"), col("duplicate_id")))
      )
      .withColumn(
        "trace_id",
        when(col("trace_id").isNull, lit(null).cast("string"))
          .otherwise(concat(col("trace_id"), lit("_copy_"), col("duplicate_id")))
      )
      .drop("duplicate_id")
  }

  private def parseArgs(args: Array[String]): Config = {
    val kvPairs = args.toList.grouped(2).collect {
      case key :: value :: Nil if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    val targetRows = kvPairs.get("target-rows")
      .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).map(_.toLong))
      .getOrElse(Seq(2000000L, 5000000L, 10000000L))

    Config(
      logsPath = kvPairs.getOrElse("logs", "data/logs.parquet"),
      deploymentsPath = kvPairs.getOrElse("deployments", "data/deployments.parquet"),
      hostMetaPath = kvPairs.getOrElse("host-meta", "data/host_meta.parquet"),
      outputRoot = kvPairs.getOrElse("output-root", "."),
      targetRows = targetRows
    )
  }
}
