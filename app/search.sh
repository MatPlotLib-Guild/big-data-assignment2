#!/bin/bash

set -euo pipefail

QUERY_TEXT="${*:-}"

if [ -z "$QUERY_TEXT" ]; then
  echo "Usage: bash search.sh \"your query here\"" >&2
  exit 1
fi

source .venv/bin/activate

RUNNING_NODES=$(yarn node -list -all 2>/dev/null | awk '$2 == "RUNNING" {count++} END {print count+0}')
if [ "$RUNNING_NODES" -lt 1 ]; then
  echo "No RUNNING YARN worker nodes are available. Restart services with bash start-services.sh." >&2
  exit 1
fi

DRIVER_PYTHON=$(which python)
EXECUTOR_PYTHON=/usr/bin/python3
SPARK_YARN_ARCHIVE="hdfs://cluster-master:9000/apps/spark/spark-jars.zip"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 1 \
  --conf spark.yarn.archive="$SPARK_YARN_ARCHIVE" \
  --conf spark.pyspark.driver.python="$DRIVER_PYTHON" \
  --conf spark.pyspark.python="$EXECUTOR_PYTHON" \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON="$EXECUTOR_PYTHON" \
  --conf spark.executorEnv.PYSPARK_PYTHON="$EXECUTOR_PYTHON" \
  query.py "$QUERY_TEXT"