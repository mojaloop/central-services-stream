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
