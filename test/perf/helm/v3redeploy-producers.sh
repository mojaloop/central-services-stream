
helm del --purge css-producer

helm install --namespace testcss --name css-producer ./cs-stream-perf -f ./v3valuesProducer.yaml
