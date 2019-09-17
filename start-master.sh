#!/bin/sh

set -eu

. ./vars.sh

rm "$SPARK_HOME/logs/*"

"$SPARK_HOME/sbin/start-master.sh"

tail -f "$SPARK_HOME/logs/spark-$USER-org.apache.spark.deploy.master.Master-1-$HOST.out"


