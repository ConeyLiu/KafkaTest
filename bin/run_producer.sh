#!/usr/bin/env bash

current_path=$(dirname "$0")
java --server -Xms2G -Xmx2G -cp ${current_path}/../target/kafkaTest-1.0-SNAPSHOT.jar com.intel.ProducerPerformance $@