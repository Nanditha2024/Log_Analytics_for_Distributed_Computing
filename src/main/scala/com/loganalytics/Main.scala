package com.loganalytics

import com.loganalytics.analytics.Analytics
import com.loganalytics.config.AppConfig
import com.loganalytics.io.DataLoader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Main {
  private def log(msg: String): Unit = println(s"[APP] $msg")

  private def showSection(title: String, df: DataFrame, limit: Int = 20): Unit = {
    println(s"========= $title =========")
    df.limit(limit).show(limit, truncate = false)
    println("")
  }

  def main(args: Array[String]): Unit = {
    AppConfig.parse(args) match {
      case Left(error) =>
        System.err.println(error)
        printUsageAndExit()
      case Right(config) =>
        val spark = SparkSession.builder()
          .appName("DistributedLogAnalytics")
          .master(sys.props.getOrElse("spark.master", "local[*]"))
          .config("spark.sql.legacy.parquet.nanosAsLong", "true")
          .config("spark.sql.shuffle.partitions", config.targetPartitions)
          .config("spark.default.parallelism", config.targetPartitions)
          .config("spark.driver.bindAddress", "127.0.0.1")
          .config("spark.driver.host", "127.0.0.1")
          .config("spark.local.ip", "127.0.0.1")
          .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        log("Spark session initialized")

        log("Loading inputs")
        val inputs = DataLoader.load(
          spark,
          config.inputFormat,
          config.logsPath,
          config.deploymentsPath,
          config.hostMetaPath
        )
        log("Inputs loaded")

        val enrichedLogs = Analytics.enrichLogs(inputs.logs, inputs.hostMeta).cache()
        log("Logs enriched with host metadata")

        log("Running session / trace reconstruction")
        val sessionOutputs = Analytics.reconstructSessions(enrichedLogs, config.sessionTimeoutMinutes)
        write(sessionOutputs.traceFlows, s"${config.outputPath}/session_trace")
        write(sessionOutputs.userSessions, s"${config.outputPath}/user_sessions")
        showSection("SESSION TRACE", sessionOutputs.traceFlows)
        showSection("USER SESSIONS", sessionOutputs.userSessions)
        log("Session / trace outputs written")

        log("Running SLO metrics")
        val slo = Analytics.sloMetrics(enrichedLogs)
        write(slo.hourly, s"${config.outputPath}/slo/hourly")
        write(slo.daily, s"${config.outputPath}/slo/daily")
        showSection("SLO HOURLY", slo.hourly)
        showSection("SLO DAILY", slo.daily)
        log("SLO outputs written")

        log("Running deployment change-impact attribution")
        val attributionStartNs = System.nanoTime()
        val attribution = Analytics.attributeToDeployment(enrichedLogs, inputs.deployments, config.attributionWindowHours)
        log("Writing change-impact attribution outputs")
        write(attribution, s"${config.outputPath}/change_impact_attribution")
        showSection("CHANGE-IMPACT ATTRIBUTION", attribution)
        val attributionSeconds = (System.nanoTime() - attributionStartNs) / 1e9
        log(f"Change-impact attribution written in $attributionSeconds%.2f s")

        log("Running anomaly detection")
        val anomalies = Analytics.detectAnomalies(enrichedLogs, config.baselineHours)
        write(anomalies.anomalies, s"${config.outputPath}/anomalies")
        write(anomalies.topOffenders, s"${config.outputPath}/top_offenders")
        showSection("ANOMALIES", anomalies.anomalies)
        showSection("TOP OFFENDERS", anomalies.topOffenders)
        log("Anomaly outputs written")

        log("Running skew benchmark")
        val skew = Analytics.skewBenchmark(enrichedLogs, config.targetPartitions, config.saltBuckets)
        write(skew, s"${config.outputPath}/skew_study")
        showSection("SKEW STUDY", skew)
        log("Skew study written")

        enrichedLogs.unpersist()
        log("Pipeline completed successfully")
        spark.stop()
    }
  }

  private def write(df: DataFrame, path: String): Unit =
    df.write.mode("overwrite").parquet(path)

  private def printUsageAndExit(): Unit = {
    System.err.println(
      """Usage: spark-submit ... --class com.loganalytics.Main <jar> [options]
        |  --logs <path>
        |  --deployments <path>
        |  --host-meta <path>
        |  --output <path>
        |  --input-format <parquet|csv|json>
        |  --session-timeout-minutes <int>
        |  --attribution-window-hours <int>
        |  --baseline-hours <int>
        |  --salt-buckets <int>
        |  --target-partitions <int>
        |  --generate-sample-data
        |  --generated-rows <long>
        |""".stripMargin
    )
    sys.exit(2)
  }
}
