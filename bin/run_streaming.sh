#!/usr/bin/env bash

CURRENT_DIR=$(dirname "$0")

{SPARK_HOME}bin/spark-submit \
          --class com.intel.Main               \
          --master         spark://master7077  \
          --deploy-mode    client              \
          ${CURRENT_DIR}/../target/kafkaTest-1.0-SNAPSHOT.jar   \
          $@

