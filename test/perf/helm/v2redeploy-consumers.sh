
helm del --purge css-prepare
helm del --purge css-position-pre
helm del --purge css-position-ful
helm del --purge css-notify-pre
helm del --purge css-notify-ful
helm del --purge css-fulfil

helm install --namespace testcss --name css-position-pre ./cs-stream-perf -f ./v2valuesConsumer-position-prepare.yaml
helm install --namespace testcss --name css-position-ful ./cs-stream-perf -f ./v2valuesConsumer-position-fulfil.yaml
helm install --namespace testcss --name css-prepare ./cs-stream-perf -f ./v2valuesConsumer-prepare.yaml
helm install --namespace testcss --name css-notify-pre ./cs-stream-perf -f ./v2valuesConsumer-notification-prepare.yaml
helm install --namespace testcss --name css-notify-ful ./cs-stream-perf -f ./v2valuesConsumer-notification-fulfil.yaml
helm install --namespace testcss --name css-fulfil ./cs-stream-perf -f ./v2valuesConsumer-fulfil.yaml
