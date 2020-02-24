#!/bin/bash

## Script to scale Mojaloop on perf1 to optimimum setup

### Config
PREPARE_SCALE=0;
FULFIL_SCALE=0;
NOTIFICATIONS_SCALE=0;
POSITION_SCALE=0;
PRODUCER_SCALE=0

### Scale Kafka Partitions
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic notification_prepare --partitions ${NOTIFICATIONS_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic notification_fulfil --partitions ${NOTIFICATIONS_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic fulfilv2 --partitions ${FULFIL_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic prepare --partitions ${PREPARE_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic position_prepare --partitions ${POSITION_SCALE};
kubectl -n backend exec -ti testclient -- ./bin/kafka-topics.sh --alter --zookeeper cssk-zookeeper.testcss:2181 --topic position_fulfil --partitions ${POSITION_SCALE};

### Scale Mojaloop Pods
kubectl -n testcss scale --replicas=${NOTIFICATIONS_SCALE} deployment/css-notify-pre-central-services-stream-perf;
kubectl -n testcss scale --replicas=${NOTIFICATIONS_SCALE} deployment/css-notify-ful-central-services-stream-perf;
kubectl -n testcss scale --replicas=${FULFIL_SCALE} deployment/css-fulfil-central-services-stream-perf;
kubectl -n testcss scale --replicas=${POSITION_SCALE} deployment/css-position-pre-central-services-stream-perf;
kubectl -n testcss scale --replicas=${POSITION_SCALE} deployment/css-position-ful-central-services-stream-perf;
kubectl -n testcss scale --replicas=${PREPARE_SCALE} deployment/css-prepare-central-services-stream-perf;
kubectl -n testcss scale --replicas=${PRODUCER_SCALE} deployment/css-producer-central-services-stream-perf;
