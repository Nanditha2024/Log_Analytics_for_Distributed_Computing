#!/usr/bin/env bash
set -euo pipefail

APP_JAR="${1:-target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar}"

for rows in 2000000 5000000 10000000; do
  out_dir="output_scale_${rows}"
  spark-submit \
    --master local[*] \
    --class com.loganalytics.Main \
    "$APP_JAR" \
    --logs "data_${rows}/logs" \
    --deployments "data_${rows}/deployments" \
    --host-meta "data_${rows}/host_meta" \
    --output "$out_dir" \
    --input-format parquet \
    --generate-sample-data \
    --generated-rows "$rows" \
    --session-timeout-minutes 30 \
    --attribution-window-hours 6 \
    --baseline-hours 24 \
    --salt-buckets 32 \
    --target-partitions 128
done
