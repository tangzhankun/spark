#!/bin/bash
export SPARK_HOME=/root/code/spark
$SPARK_HOME/bin/spark-submit \
  --class "org.apache.spark.examples.mllib.RankingMetricsExample" \
  --master local[1] \
  --driver-memory 6G \
  examples/target/original-spark-examples_2.11-2.2.1-SNAPSHOT.jar  
