
helm del --purge css-prepare
helm del --purge css-position
helm del --purge css-notify
helm del --purge css-fulfil

# helm install --namespace testcss --name css-position ./cs-stream-perf -f ./valuesConsumer-position.yaml
helm install --namespace testcss --name css-prepare ./cs-stream-perf -f ./valuesConsumer-prepare.yaml
# helm install --namespace testcss --name css-notify ./cs-stream-perf -f ./valuesConsumer-notification.yaml
# helm install --namespace testcss --name css-fulfil ./cs-stream-perf -f ./valuesConsumer-fulfil.yaml
