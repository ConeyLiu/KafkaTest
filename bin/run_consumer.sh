#!/usr/bin/env bash

CURRENT_DIR=$(dirname "$0")

JVM_OPTS="-server -Xms20G -Xmx20G -XX:InitialBootClassLoaderMetaspaceSize=128m -XX:+UseParallelGC -XX:+UseParallelOldGC -XX:SurvivorRatio=4"

java ${JVM_OPTS} -cp ${CURRENT_DIR}/../target/kafkaTest-1.0-SNAPSHOT.jar com.intel.consumer.ConsumerPerformance $@