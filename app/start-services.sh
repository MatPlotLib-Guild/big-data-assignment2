#!/bin/bash
# This will run only by the master node

# starting HDFS daemons
$HADOOP_HOME/sbin/start-dfs.sh

# starting Yarn daemons
$HADOOP_HOME/sbin/start-yarn.sh

# Keep a local NodeManager on the master so the Spark AM always has a stable place
# to start even if one of the worker NodeManagers is flaky.
yarn --daemon start nodemanager

# Start mapreduce history server
mapred --daemon start historyserver


# track process IDs of services
jps -lm

# subtool to perform administrator functions on HDFS
# outputs a brief report on the overall HDFS filesystem
hdfs dfsadmin -report

# If namenode in safemode then leave it
hdfs dfsadmin -safemode leave

# Create a compact Spark runtime archive for YARN. Uploading a single archive is
# much more reliable here than making YARN localize hundreds of individual jars.
hdfs dfs -mkdir -p /apps/spark
python3 - <<'PY'
from pathlib import Path
import zipfile

archive_path = Path("/tmp/spark-jars.zip")
jars_dir = Path("/usr/local/spark/jars")

with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
    for jar_path in sorted(jars_dir.glob("*.jar")):
        archive.write(jar_path, arcname=jar_path.name)
PY
hdfs dfs -put -f /tmp/spark-jars.zip /apps/spark/spark-jars.zip


# print version of Scala of Spark
scala -version

# track process IDs of services
jps -lm

# Create a directory for root user on HDFS
hdfs dfs -mkdir -p /user/root

