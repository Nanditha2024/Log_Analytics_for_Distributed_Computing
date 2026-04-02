#!/usr/bin/env bash
set -euo pipefail

APP_JAR="${1:-target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar}"

spark-submit \
  --master local[*] \
  --class com.loganalytics.Main \
  "$APP_JAR" \
  --logs data/logs.parquet \
  --deployments data/deployments.parquet \
  --host-meta data/host_meta.parquet \
  --output output \
  --input-format parquet \
  --session-timeout-minutes 30 \
  --attribution-window-hours 6 \
  --baseline-hours 24 \
  --salt-buckets 16 \
  --target-partitions 64
