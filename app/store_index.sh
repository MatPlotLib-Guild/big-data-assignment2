#!/bin/bash

set -euo pipefail

INDEX_ROOT="${1:-/indexer}"
CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra-server}"
KEYSPACE="${KEYSPACE:-search_engine}"
WAIT_ATTEMPTS="${WAIT_ATTEMPTS:-24}"
WAIT_SECONDS="${WAIT_SECONDS:-5}"
LOCAL_TMP="$(mktemp -d /tmp/store_index.XXXXXX)"

cleanup() {
  rm -rf "$LOCAL_TMP"
}

trap cleanup EXIT

echo "Store index from HDFS into Cassandra"
echo "Index root: $INDEX_ROOT"
echo "Cassandra host: $CASSANDRA_HOST"
echo "Keyspace: $KEYSPACE"

hdfs dfs -test -e "$INDEX_ROOT/index"
hdfs dfs -test -e "$INDEX_ROOT/doc_stats"
hdfs dfs -test -e "$INDEX_ROOT/corpus_stats"
hdfs dfs -test -e "$INDEX_ROOT/vocabulary"

echo "Copying HDFS index files to local staging area"
hdfs dfs -cat "$INDEX_ROOT/index"/part-* > "$LOCAL_TMP/postings.tsv"
hdfs dfs -cat "$INDEX_ROOT/doc_stats"/part-* > "$LOCAL_TMP/doc_stats.tsv"
hdfs dfs -cat "$INDEX_ROOT/corpus_stats"/part-* > "$LOCAL_TMP/corpus_stats.tsv"
hdfs dfs -cat "$INDEX_ROOT/vocabulary"/part-* > "$LOCAL_TMP/vocabulary.tsv"

echo "Waiting for Cassandra to accept connections"
for attempt in $(seq 1 "$WAIT_ATTEMPTS"); do
  if python3 - "$CASSANDRA_HOST" <<'PY'
from cassandra.cluster import Cluster
import sys

cluster = Cluster([sys.argv[1]])
session = cluster.connect()
session.shutdown()
cluster.shutdown()
PY
  then
    break
  fi

  if [ "$attempt" -eq "$WAIT_ATTEMPTS" ]; then
    echo "Cassandra did not become ready in time" >&2
    exit 1
  fi

  echo "Cassandra is not ready yet (attempt $attempt/$WAIT_ATTEMPTS)"
  sleep "$WAIT_SECONDS"
done

python3 store_index.py "$LOCAL_TMP" "$CASSANDRA_HOST" "$KEYSPACE"
