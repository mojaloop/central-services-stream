#!/bin/bash

## Script to scale Mojaloop on perf1 to optimimum setup

### Config
PREPARE_SCALE=8;
FULFIL_SCALE=8;
NOTIFICATIONS_SCALE=16;
POSITION_SCALE=16;
PRODUCER_SCALE=0

### Scale Kafka Partitions
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic notification --partitions ${NOTIFICATIONS_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic fulfil --partitions ${FULFIL_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic prepare --partitions ${PREPARE_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic position --partitions ${POSITION_SCALE};

### Scale Mojaloop Pods
kubectl -n testcss scale --replicas=${NOTIFICATIONS_SCALE} deployment/css-notify-central-services-stream-perf;
kubectl -n testcss scale --replicas=${FULFIL_SCALE} deployment/css-fulfil-central-services-stream-perf;
kubectl -n testcss scale --replicas=${POSITION_SCALE} deployment/css-position-central-services-stream-perf;
kubectl -n testcss scale --replicas=${PREPARE_SCALE} deployment/css-prepare-central-services-stream-perf;
kubectl -n testcss scale --replicas=${PRODUCER_SCALE} deployment/css-producer-central-services-stream-perf;
