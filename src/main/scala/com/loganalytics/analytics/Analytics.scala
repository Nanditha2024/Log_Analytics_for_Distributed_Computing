package com.loganalytics.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Analytics {
  final case class SessionOutputs(traceFlows: DataFrame, userSessions: DataFrame)
  final case class SloOutputs(hourly: DataFrame, daily: DataFrame)
  final case class AnomalyOutputs(anomalies: DataFrame, topOffenders: DataFrame)

  def enrichLogs(logs: DataFrame, hostMeta: DataFrame): DataFrame =
    logs.join(hostMeta, Seq("host"), "left")
      .withColumn("region", coalesce(col("region"), lit("unknown")))
      .withColumn("instance_type", coalesce(col("instance_type"), lit("unknown")))

  def reconstructSessions(logs: DataFrame, timeoutMinutes: Int): SessionOutputs = {
    val traceFlows = logs
      .filter(col("trace_id").isNotNull)
      .groupBy("trace_id")
      .agg(
        min("timestamp").as("trace_start"),
        max("timestamp").as("trace_end"),
        count(lit(1)).as("event_count"),
        sort_array(
          collect_list(
            struct(
              col("timestamp"),
              col("service"),
              col("endpoint"),
              col("status_code"),
              col("latency_ms"),
              col("host"),
              col("region")
            )
          )
        ).as("request_flow")
      )

    val userWindow = Window.partitionBy("user_id").orderBy("timestamp")
    val markSessionStart = logs
      .filter(col("user_id").isNotNull)
      .withColumn("prev_ts", lag(col("timestamp"), 1).over(userWindow))
      .withColumn("gap_minutes", (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_ts"))) / 60.0)
      .withColumn(
        "is_new_session",
        when(col("prev_ts").isNull || col("gap_minutes") > lit(timeoutMinutes.toDouble), lit(1)).otherwise(lit(0))
      )
      .withColumn(
        "session_index",
        sum("is_new_session").over(userWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow))
      )
      .withColumn("session_id", concat_ws("-", col("user_id"), lpad(col("session_index").cast("string"), 8, "0")))

    val userSessions = markSessionStart.groupBy("session_id", "user_id")
      .agg(
        min("timestamp").as("session_start"),
        max("timestamp").as("session_end"),
        count(lit(1)).as("event_count"),
        avg("latency_ms").as("avg_latency_ms"),
        expr("percentile_approx(latency_ms, 0.95, 1000)").as("p95_latency_ms"),
        avg(when(col("status_code") >= 500, 1.0).otherwise(0.0)).as("error_rate"),
        sort_array(
          collect_list(
            struct(
              col("timestamp"),
              col("service"),
              col("endpoint"),
              col("status_code"),
              col("latency_ms"),
              col("trace_id")
            )
          )
        ).as("request_flow")
      )

    SessionOutputs(traceFlows, userSessions)
  }

  def sloMetrics(logs: DataFrame): SloOutputs = {
    val withBuckets = logs
      .withColumn("hour_bucket", date_trunc("hour", col("timestamp")))
      .withColumn("day_bucket", to_date(col("timestamp")))
      .withColumn("is_error", when(col("status_code") >= 500, lit(1.0)).otherwise(lit(0.0)))

    def aggBy(timeCol: String): DataFrame =
      withBuckets.groupBy(col("service"), col("endpoint"), col("region"), col(timeCol))
        .agg(
          count(lit(1)).as("request_count"),
          avg("latency_ms").as("avg_latency_ms"),
          expr("percentile_approx(latency_ms, array(0.50, 0.95, 0.99), 1000)").as("latency_percentiles"),
          avg("is_error").as("error_rate")
        )
        .select(
          col("service"),
          col("endpoint"),
          col("region"),
          col(timeCol).as("time_bucket"),
          col("request_count"),
          col("avg_latency_ms"),
          col("latency_percentiles").getItem(0).as("p50_latency_ms"),
          col("latency_percentiles").getItem(1).as("p95_latency_ms"),
          col("latency_percentiles").getItem(2).as("p99_latency_ms"),
          col("error_rate")
        )

    SloOutputs(hourly = aggBy("hour_bucket"), daily = aggBy("day_bucket"))
  }

  def attributeToDeployment(logs: DataFrame, deployments: DataFrame, attributionWindowHours: Int): DataFrame = {
    val logsHourly = logs
      .withColumn("time_bucket", date_trunc("hour", col("timestamp")))
      .groupBy("service", "endpoint", "region", "time_bucket")
      .agg(
        count(lit(1)).as("request_count"),
        avg("latency_ms").as("avg_latency_ms"),
        expr("percentile_approx(latency_ms, 0.95, 1000)").as("p95_latency_ms"),
        avg(when(col("status_code") >= 500, 1.0).otherwise(0.0)).as("error_rate")
      )

    val deploymentW = Window.partitionBy("service").orderBy("deploy_time")
    val deploymentsWithIntervals = broadcast(
      deployments
        .select(col("service"), col("version"), col("deploy_time"))
        .where(col("deploy_time").isNotNull)
        .withColumn("next_deploy_time", lead(col("deploy_time"), 1).over(deploymentW))
    )

    logsHourly.alias("l")
      .join(
        deploymentsWithIntervals.alias("d"),
        col("l.service") === col("d.service") &&
          col("l.time_bucket") >= col("d.deploy_time") &&
          (col("d.next_deploy_time").isNull || col("l.time_bucket") < col("d.next_deploy_time")) &&
          col("l.time_bucket") <= expr(s"d.deploy_time + INTERVAL $attributionWindowHours HOURS"),
        "left"
      )
      .withColumn(
        "minutes_since_deploy",
        when(col("d.deploy_time").isNull, lit(null).cast("double"))
          .otherwise((unix_timestamp(col("l.time_bucket")) - unix_timestamp(col("d.deploy_time"))) / 60.0)
      )
      .select(
        col("l.time_bucket").as("time_bucket"),
        col("l.service").as("service"),
        col("l.endpoint").as("endpoint"),
        col("l.region").as("region"),
        col("l.request_count").as("request_count"),
        col("l.avg_latency_ms").as("avg_latency_ms"),
        col("l.p95_latency_ms").as("p95_latency_ms"),
        col("l.error_rate").as("error_rate"),
        col("d.version").as("deploy_version"),
        col("d.deploy_time").as("deploy_time"),
        col("minutes_since_deploy")
      )
  }

  def detectAnomalies(logs: DataFrame, baselineHours: Int): AnomalyOutputs = {
    val hourly = logs
      .withColumn("hour_bucket", date_trunc("hour", col("timestamp")))
      .groupBy("service", "endpoint", "region", "hour_bucket")
      .agg(
        count(lit(1)).as("request_count"),
        avg(when(col("status_code") >= 500, 1.0).otherwise(0.0)).as("error_rate"),
        expr("percentile_approx(latency_ms, 0.95, 1000)").as("p95_latency_ms")
      )

    val baselineW = Window.partitionBy("service", "endpoint", "region")
      .orderBy("hour_bucket")
      .rowsBetween(-baselineHours, -1)

    val scored = hourly
      .withColumn("baseline_error_avg", avg("error_rate").over(baselineW))
      .withColumn("baseline_error_std", stddev_pop("error_rate").over(baselineW))
      .withColumn("baseline_p95_avg", avg("p95_latency_ms").over(baselineW))
      .withColumn("baseline_p95_std", stddev_pop("p95_latency_ms").over(baselineW))
      .withColumn(
        "error_zscore",
        when(col("baseline_error_std") > 0.0, (col("error_rate") - col("baseline_error_avg")) / col("baseline_error_std"))
      )
      .withColumn(
        "p95_zscore",
        when(col("baseline_p95_std") > 0.0, (col("p95_latency_ms") - col("baseline_p95_avg")) / col("baseline_p95_std"))
      )
      .withColumn(
        "error_anomaly",
        col("baseline_error_avg").isNotNull && (col("error_rate") > col("baseline_error_avg") + (lit(3.0) * col("baseline_error_std")))
      )
      .withColumn(
        "latency_anomaly",
        col("baseline_p95_avg").isNotNull && (col("p95_latency_ms") > col("baseline_p95_avg") + (lit(3.0) * col("baseline_p95_std")))
      )
      .withColumn(
        "severity",
        greatest(coalesce(col("error_zscore"), lit(0.0)), coalesce(col("p95_zscore"), lit(0.0)))
      )
      .withColumn(
        "explanation",
        concat_ws(
          "; ",
          when(col("error_anomaly"), format_string("error_rate %.4f > baseline %.4f", col("error_rate"), col("baseline_error_avg"))),
          when(col("latency_anomaly"), format_string("p95 %.2fms > baseline %.2fms", col("p95_latency_ms"), col("baseline_p95_avg")))
        )
      )

    val anomalies = scored.filter(col("error_anomaly") || col("latency_anomaly"))

    val topOffenders = anomalies
      .groupBy("service", "endpoint", "region")
      .agg(
        count(lit(1)).as("anomaly_windows"),
        max("severity").as("max_severity"),
        max("p95_latency_ms").as("peak_p95_ms"),
        max("error_rate").as("peak_error_rate"),
        concat_ws(" | ", collect_set("explanation")).as("explanations")
      )
      .orderBy(col("max_severity").desc, col("anomaly_windows").desc)

    AnomalyOutputs(anomalies, topOffenders)
  }

  def skewBenchmark(logs: DataFrame, targetPartitions: Int, saltBuckets: Int): DataFrame = {
    val totalCount = logs.count().toDouble
    val topEndpointRow = logs.groupBy("endpoint").count().orderBy(col("count").desc).limit(1)
    val topEndpoint = topEndpointRow.select("endpoint").head().getString(0)
    val topEndpointCount = topEndpointRow.select("count").head().getLong(0)
    val topEndpointShare = if (totalCount == 0) 0.0 else topEndpointCount / totalCount

    val beforePrepared = logs.repartition(targetPartitions, col("endpoint"))
    val beforePartitionSkew = beforePrepared
      .withColumn("pid", spark_partition_id())
      .groupBy("pid")
      .count()
      .agg(
        max("count").as("before_max_partition_rows"),
        min("count").as("before_min_partition_rows"),
        avg("count").as("before_avg_partition_rows")
      )

    val beforeStart = System.nanoTime()
    beforePrepared
      .groupBy("endpoint")
      .agg(
        count(lit(1)).as("request_count"),
        avg("latency_ms").as("avg_latency_ms"),
        expr("percentile_approx(latency_ms, 0.95, 1000)").as("p95_latency_ms")
      )
      .count()
    val beforeMs = (System.nanoTime() - beforeStart) / 1000000L

    val salted = logs.withColumn(
      "salt",
      when(
        col("endpoint") === lit(topEndpoint),
        pmod(xxhash64(coalesce(col("trace_id"), col("user_id"), col("host"))), lit(math.max(saltBuckets, 1)))
      ).otherwise(lit(0))
    )

    val afterPrepared = salted.repartition(targetPartitions, col("endpoint"), col("salt"))
    val afterPartitionSkew = afterPrepared
      .withColumn("pid", spark_partition_id())
      .groupBy("pid")
      .count()
      .agg(
        max("count").as("after_max_partition_rows"),
        min("count").as("after_min_partition_rows"),
        avg("count").as("after_avg_partition_rows")
      )

    val afterStart = System.nanoTime()
    afterPrepared
      .groupBy("endpoint", "salt")
      .agg(
        count(lit(1)).as("partial_count"),
        sum("latency_ms").as("partial_latency_sum")
      )
      .groupBy("endpoint")
      .agg(
        sum("partial_count").as("request_count"),
        (sum("partial_latency_sum") / sum("partial_count")).as("avg_latency_ms")
      )
      .count()
    val afterMs = (System.nanoTime() - afterStart) / 1000000L

    beforePartitionSkew.crossJoin(afterPartitionSkew)
      .withColumn("top_endpoint", lit(topEndpoint))
      .withColumn("top_endpoint_share", lit(topEndpointShare))
      .withColumn("salt_buckets", lit(saltBuckets))
      .withColumn("target_partitions", lit(targetPartitions))
      .withColumn("runtime_before_ms", lit(beforeMs))
      .withColumn("runtime_after_ms", lit(afterMs))
      .withColumn("runtime_improvement_pct", lit((beforeMs - afterMs) * 100.0 / math.max(beforeMs, 1L)))
  }
}
