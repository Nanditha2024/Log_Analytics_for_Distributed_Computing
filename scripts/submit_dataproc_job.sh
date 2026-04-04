#!/usr/bin/env bash
set -euo pipefail

required_vars=(
  GCP_PROJECT_ID
  GCP_REGION
  DATAPROC_CLUSTER
  DATAPROC_STAGING_BUCKET
  GCS_LOGS_PATH
  GCS_DEPLOYMENTS_PATH
  GCS_HOST_META_PATH
  GCS_OUTPUT_BASE
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "Missing required env var: $var" >&2
    exit 1
  fi
done

jar_path="${1:-target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar}"

if [[ ! -f "$jar_path" ]]; then
  echo "Jar file not found: $jar_path" >&2
  exit 1
fi

commit_sha="${GITHUB_SHA:-local}"
short_sha="${commit_sha:0:7}"
jar_name="distributed-log-analytics-${short_sha}.jar"
remote_jar_uri="gs://${DATAPROC_STAGING_BUCKET}/artifacts/${jar_name}"

run_suffix="$(date +%Y%m%d-%H%M%S)"
output_path="${GCS_OUTPUT_BASE%/}/run-${short_sha}-${run_suffix}"

session_timeout_minutes="${SESSION_TIMEOUT_MINUTES:-30}"
attribution_window_hours="${ATTRIBUTION_WINDOW_HOURS:-6}"
baseline_hours="${BASELINE_HOURS:-24}"
salt_buckets="${SALT_BUCKETS:-16}"
target_partitions="${TARGET_PARTITIONS:-64}"

echo "Uploading jar to ${remote_jar_uri}"
gsutil cp "$jar_path" "$remote_jar_uri"

echo "Submitting Dataproc Spark job to cluster ${DATAPROC_CLUSTER} in ${GCP_REGION}"
gcloud dataproc jobs submit spark \
  --project "$GCP_PROJECT_ID" \
  --region "$GCP_REGION" \
  --cluster "$DATAPROC_CLUSTER" \
  --jars "$remote_jar_uri" \
  --class com.loganalytics.Main \
  -- \
  --logs "$GCS_LOGS_PATH" \
  --deployments "$GCS_DEPLOYMENTS_PATH" \
  --host-meta "$GCS_HOST_META_PATH" \
  --output "$output_path" \
  --input-format parquet \
  --session-timeout-minutes "$session_timeout_minutes" \
  --attribution-window-hours "$attribution_window_hours" \
  --baseline-hours "$baseline_hours" \
  --salt-buckets "$salt_buckets" \
  --target-partitions "$target_partitions"

echo "Dataproc job submitted successfully"
