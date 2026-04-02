package com.loganalytics.config

final case class AppConfig(
    logsPath: String = "data/logs.parquet",
    deploymentsPath: String = "data/deployments.parquet",
    hostMetaPath: String = "data/host_meta.parquet",
    outputPath: String = "output",
    inputFormat: String = "parquet",
    sessionTimeoutMinutes: Int = 30,
    attributionWindowHours: Int = 6,
    baselineHours: Int = 24,
    saltBuckets: Int = 16,
    targetPartitions: Int = 64,
    generateSampleData: Boolean = false,
    generatedRows: Long = 2000000L
)

object AppConfig {
  def parse(args: Array[String]): Either[String, AppConfig] = {
    val kvPairs = args.toList.grouped(2).collect {
      case key :: value :: Nil if key.startsWith("--") => key.drop(2) -> value
    }.toMap

    val flags = args.filter(_.startsWith("--")).toSet

    def intArg(name: String, default: Int): Int =
      kvPairs.get(name).flatMap(v => scala.util.Try(v.toInt).toOption).getOrElse(default)

    def longArg(name: String, default: Long): Long =
      kvPairs.get(name).flatMap(v => scala.util.Try(v.toLong).toOption).getOrElse(default)

    val unknown = flags.filterNot(flag =>
      Set(
        "--logs",
        "--deployments",
        "--host-meta",
        "--output",
        "--input-format",
        "--session-timeout-minutes",
        "--attribution-window-hours",
        "--baseline-hours",
        "--salt-buckets",
        "--target-partitions",
        "--generate-sample-data",
        "--generated-rows"
      ).contains(flag)
    )

    if (unknown.nonEmpty) {
      Left(s"Unknown arguments: ${unknown.mkString(", ")}")
    } else {
      Right(
        AppConfig(
          logsPath = kvPairs.getOrElse("logs", "data/logs.parquet"),
          deploymentsPath = kvPairs.getOrElse("deployments", "data/deployments.parquet"),
          hostMetaPath = kvPairs.getOrElse("host-meta", "data/host_meta.parquet"),
          outputPath = kvPairs.getOrElse("output", "output"),
          inputFormat = kvPairs.getOrElse("input-format", "parquet"),
          sessionTimeoutMinutes = intArg("session-timeout-minutes", 30),
          attributionWindowHours = intArg("attribution-window-hours", 6),
          baselineHours = intArg("baseline-hours", 24),
          saltBuckets = intArg("salt-buckets", 16),
          targetPartitions = intArg("target-partitions", 64),
          generateSampleData = flags.contains("--generate-sample-data"),
          generatedRows = longArg("generated-rows", 2000000L)
        )
      )
    }
  }
}
