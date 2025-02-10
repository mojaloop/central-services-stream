const SemConv = Object.freeze({ // https://www.npmjs.com/package/@opentelemetry/semantic-conventions#unstable-semconv
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT: 'messaging.batch.message_count',
  ATTR_MESSAGING_CLIENT_ID: 'messaging.client.id',
  ATTR_MESSAGING_CONSUMER_GROUP_NAME: 'messaging.consumer.group.name',
  ATTR_MESSAGING_DESTINATION_NAME: 'messaging.destination.name',
  ATTR_MESSAGING_OPERATION_NAME: 'messaging.operation.name',
  ATTR_MESSAGING_SYSTEM: 'messaging.system',
  ATTR_SERVER_ADDRESS: 'server.address'
})

const OTEL_HEADERS = ['traceparent', 'tracestate', 'baggage']

module.exports = {
  SemConv,
  OTEL_HEADERS
}
