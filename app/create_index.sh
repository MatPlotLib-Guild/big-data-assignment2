#!/bin/bash

set -euo pipefail

INPUT_PATH="${1:-/input/data}"
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar)
TMP_ROOT="/tmp/indexer"
PIPELINE1_OUTPUT="$TMP_ROOT/pipeline1"
LOCAL_INDEX_OUTPUT="/tmp/indexer_local"
MAP_LIMIT="${MAP_LIMIT:-2}"


ensure_yarn() {
  if yarn node -list >/dev/null 2>&1; then
    return
  fi

  echo "YARN is not responding; attempting to start ResourceManager"
  yarn --daemon start resourcemanager
  sleep 5

  if ! yarn node -list >/dev/null 2>&1; then
    echo "ResourceManager is still unavailable. The cluster master was likely OOM-killed." >&2
    echo "Try lowering Docker Desktop memory pressure or rerun with fewer concurrent maps." >&2
    exit 1
  fi
}

echo "Create index using MapReduce pipelines"
echo "Input path: $INPUT_PATH"
echo "Concurrent mapper limit: $MAP_LIMIT"

hdfs dfs -test -d "$INPUT_PATH"
ensure_yarn

hdfs dfs -rm -r -f "$TMP_ROOT" /indexer/index /indexer/doc_stats /indexer/corpus_stats /indexer/vocabulary
hdfs dfs -mkdir -p "$TMP_ROOT" /indexer

echo "Running pipeline 1: postings and document statistics"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline1" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.job.running.map.limit="$MAP_LIMIT" \
  -D mapreduce.job.reduce.slowstart.completedmaps=1.0 \
  -D mapreduce.map.memory.mb=256 \
  -D mapreduce.map.java.opts=-Xmx200m \
  -D mapreduce.reduce.memory.mb=512 \
  -D stream.map.output.field.separator=$'\t' \
  -D stream.num.map.output.key.fields=4 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$PIPELINE1_OUTPUT"

echo "Materializing pipeline 1 outputs under /indexer"
rm -rf "$LOCAL_INDEX_OUTPUT"
mkdir -p "$LOCAL_INDEX_OUTPUT"

hdfs dfs -cat "$PIPELINE1_OUTPUT"/part-* | python3 split_index_output.py "$LOCAL_INDEX_OUTPUT"

hdfs dfs -mkdir -p /indexer/index /indexer/doc_stats /indexer/corpus_stats
hdfs dfs -put -f "$LOCAL_INDEX_OUTPUT/postings.tsv" /indexer/index/part-00000
hdfs dfs -put -f "$LOCAL_INDEX_OUTPUT/doc_stats.tsv" /indexer/doc_stats/part-00000
hdfs dfs -put -f "$LOCAL_INDEX_OUTPUT/corpus_stats.tsv" /indexer/corpus_stats/part-00000

echo "Running pipeline 2: vocabulary with document frequencies"
ensure_yarn
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name="indexer-pipeline2" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.map.memory.mb=256 \
  -D mapreduce.map.java.opts=-Xmx200m \
  -D stream.map.output.field.separator=$'\t' \
  -D stream.num.map.output.key.fields=2 \
  -files mapreduce/mapper2.py,mapreduce/reducer2.py \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py" \
  -input "$PIPELINE1_OUTPUT" \
  -output /indexer/vocabulary

echo "Final HDFS outputs:"
hdfs dfs -ls /indexer
