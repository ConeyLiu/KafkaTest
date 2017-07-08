#!/usr/bin/env bash

CURRENT_DIR=$(dirname "$0")
SPARK_HOME=/home/xianyang/opt/spark
${SPARK_HOME}/bin/spark-submit \
          --class com.intel.Main               \
          --master         yarn                \
          --deploy-mode    client              \
          --executor-memory 30G               \
          --executor-cores  10                \
          --num-executors 3                    \
          ${CURRENT_DIR}/../target/kafkaTest-1.0-SNAPSHOT.jar   \
          $@

