#!/bin/sh

set -eu

. ./vars.sh

"$SPARK_HOME/sbin/start-slave.sh" "spark://$HOST:7077"

tail -f "$SPARK_HOME/logs/spark-$USER-org.apache.spark.deploy.worker.Worker-1-$HOST.out"

