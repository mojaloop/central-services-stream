
helm del --purge css-prepare
helm del --purge css-position
helm del --purge css-notify
helm del --purge css-fulfil

helm install --namespace testcss --name css-position ./cs-stream-perf -f ./v3valuesConsumer-position.yaml
helm install --namespace testcss --name css-prepare ./cs-stream-perf -f ./v3valuesConsumer-prepare.yaml
helm install --namespace testcss --name css-notify ./cs-stream-perf -f ./v3valuesConsumer-notification.yaml
helm install --namespace testcss --name css-fulfil ./cs-stream-perf -f ./v3valuesConsumer-fulfil.yaml
