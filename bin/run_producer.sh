#!/usr/bin/env bash

current_dir = dirname $0
java -Xms 2G -Xmx 2G -cp ${current_path}../target/kafkaTest-1.0-SNAPSHOT.jar com.intel.ProducerPerformance $@