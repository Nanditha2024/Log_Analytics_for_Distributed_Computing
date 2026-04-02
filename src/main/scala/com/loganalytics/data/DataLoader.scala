package com.loganalytics.io

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataLoader {
  final case class Inputs(logs: DataFrame, deployments: DataFrame, hostMeta: DataFrame)

  private def normalizeTimestamp(df: DataFrame, columnName: String): DataFrame = {
    val dataType = df.schema(columnName).dataType
    dataType match {
      case TimestampType =>
        df.withColumn(columnName, col(columnName).cast("timestamp"))
      case LongType =>
        df.withColumn(columnName, to_timestamp(from_unixtime((col(columnName) / lit(1000000000L)).cast("double"))))
      case IntegerType =>
        df.withColumn(columnName, to_timestamp(from_unixtime(col(columnName).cast("long"))))
      case StringType =>
        df.withColumn(columnName, to_timestamp(col(columnName)))
      case _ =>
        df.withColumn(columnName, to_timestamp(col(columnName).cast("string")))
    }
  }

  private def readInput(spark: SparkSession, format: String, path: String): DataFrame = {
    val reader = spark.read.format(format)
    if (format.equalsIgnoreCase("csv")) {
      reader.option("header", "true").option("inferSchema", "true").load(path)
    } else {
      reader.load(path)
    }
  }

  def load(spark: SparkSession, format: String, logsPath: String, deploymentsPath: String, hostMetaPath: String): Inputs = {
    val logsRaw = readInput(spark, format, logsPath)
    val logs = normalizeTimestamp(logsRaw, "timestamp")
      .withColumn("service", col("service").cast("string"))
      .withColumn("host", col("host").cast("string"))
      .withColumn("endpoint", col("endpoint").cast("string"))
      .withColumn("status_code", col("status_code").cast("int"))
      .withColumn("latency_ms", col("latency_ms").cast("double"))
      .withColumn("user_id", col("user_id").cast("string"))
      .withColumn("trace_id", col("trace_id").cast("string"))

    val deploymentsRaw = readInput(spark, format, deploymentsPath)
    val deployments = normalizeTimestamp(deploymentsRaw, "deploy_time")
      .withColumn("service", col("service").cast("string"))
      .withColumn("version", col("version").cast("string"))

    val hostMeta = readInput(spark, format, hostMetaPath)
      .withColumn("host", col("host").cast("string"))
      .withColumn("region", col("region").cast("string"))
      .withColumn("instance_type", col("instance_type").cast("string"))

    Inputs(logs, deployments, hostMeta)
  }
}
