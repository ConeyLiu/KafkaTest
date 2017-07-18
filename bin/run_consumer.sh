#!/usr/bin/env bash

CURRENT_DIR=$(dirname "$0")
java -server -Xms2G -Xmx2G -cp ${CURRENT_DIR}/../target/kafkaTest-1.0-SNAPSHOT.jar com.intel.consumer.ConsumerPerformance $@