#!/bin/bash

set -e

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

rm -f data/*.txt

hdfs dfs -put -f data/a.parquet / && \
    spark-submit prepare_data.py && \
    echo "Putting text documents to hdfs" && \
    hdfs dfs -rm -r -f /input/data && \
    hdfs dfs -mkdir -p /input/data && \
    hdfs dfs -put data/*.txt /input/data/ && \
    hdfs dfs -ls /input/data && \
    echo "done data preparation!"
