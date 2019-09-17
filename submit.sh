#!/bin/sh

set -eu

. ./vars.sh

"$SPARK_HOME/bin/spark-submit" \
  --class "com.opsian.spark_examples.${CLASS}" \
  --master "spark://$HOST:7077" \
  --executor-memory 2G \
  --total-executor-cores 8 \
  --conf "spark.executor.extraJavaOptions=-agentpath:$PWD/libopsian.so=agentId=${AGENT_ID}_executor,apiKey=$KEY" \
  --conf "spark.driver.extraJavaOptions=-agentpath:$PWD/libopsian.so=agentId=${AGENT_ID}_driver,apiKey=$KEY" \
  target/spark_examples-1.0-SNAPSHOT.jar

