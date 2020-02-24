# Running Test Scripts

## Run Consumer:
```bash
helm install --namespace testcss --name css-consumer ./cs-stream-perf -f ./valuesConsumer.yaml
```

## Del Consumer
```bash
helm del --purge css-consumer
```

## Run Producer:
```bash
helm install --namespace testcss --name css-producer ./cs-stream-perf -f ./valuesProducer.yaml
```

## Del Produder
```bash
helm del --purge css-producer
```

## CLI Command to list consumers of a consumer group1
kubectl -n backend exec -ti testclient -- ./bin/kafka-consumer-groups.sh --bootstrap-server cssk-kafka.testcss:9092 --group group1 --describe
