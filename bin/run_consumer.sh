#!/usr/bin/env bash

current_path=$(dirname "$0")
java -Xms 2G -Xmx 2G -cp ${current_path}/../target/kafkaTest-1.0-SNAPSHOT.jar com.intel.ConsumerPerformance $@