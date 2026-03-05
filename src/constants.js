// Incubating OTel semantic conventions for messaging (not yet in stable @opentelemetry/semantic-conventions)
const SemConv = Object.freeze({
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT: 'messaging.batch.message_count',
  ATTR_MESSAGING_CLIENT_ID: 'messaging.client.id',
  ATTR_MESSAGING_CONSUMER_GROUP_NAME: 'messaging.consumer.group.name',
  ATTR_MESSAGING_DESTINATION_NAME: 'messaging.destination.name',
  ATTR_MESSAGING_OPERATION_NAME: 'messaging.operation.name',
  ATTR_MESSAGING_OPERATION_TYPE: 'messaging.operation.type',
  ATTR_MESSAGING_SYSTEM: 'messaging.system',
  ATTR_MESSAGING_DESTINATION_PARTITION_ID: 'messaging.destination.partition.id',
  ATTR_MESSAGING_KAFKA_OFFSET: 'messaging.kafka.offset',
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY: 'messaging.kafka.message.key'
})

const SpanPrefixes = Object.freeze({
  RECEIVE: 'RECEIVE',
  SEND: 'SEND'
})

const OTEL_HEADERS = ['traceparent', 'tracestate', 'baggage']

const stateList = {
  PENDING: 'PENDING',
  DOWN: 'DOWN',
  OK: 'OK'
}

const kafkaBrokerStates = {
  INIT: 'INIT',
  DOWN: 'DOWN',
  TRY_CONNECT: 'TRY_CONNECT',
  CONNECT: 'CONNECT',
  SSL_HANDSHAKE: 'SSL_HANDSHAKE',
  AUTH_LEGACY: 'AUTH_LEGACY',
  AUTH: 'AUTH',
  UP: 'UP',
  UPDATE: 'UPDATE'
}

module.exports = {
  SemConv,
  SpanPrefixes,
  OTEL_HEADERS,
  stateList,
  kafkaBrokerStates
}
