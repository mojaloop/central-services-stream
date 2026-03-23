const { ATTR_SERVER_ADDRESS, ATTR_ERROR_TYPE } = require('@opentelemetry/semantic-conventions')

const SemConv = Object.freeze({
  ATTR_SERVER_ADDRESS,
  ATTR_ERROR_TYPE,
  // messaging OTel semantic conventions are not yet in stable @opentelemetry/semantic-conventions
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT: 'messaging.batch.message_count',
  ATTR_MESSAGING_CLIENT_ID: 'messaging.client.id',
  ATTR_MESSAGING_CONSUMER_GROUP_NAME: 'messaging.consumer.group.name',
  ATTR_MESSAGING_DESTINATION_NAME: 'messaging.destination.name',
  ATTR_MESSAGING_OPERATION_NAME: 'messaging.operation.name',
  ATTR_MESSAGING_SYSTEM: 'messaging.system',
  ATTR_MESSAGING_DESTINATION_PARTITION_ID: 'messaging.destination.partition.id',
  ATTR_MESSAGING_KAFKA_OFFSET: 'messaging.kafka.offset',
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY: 'messaging.kafka.message.key'
})

const SpanPrefixes = Object.freeze({
  SEND: 'SEND',
  RECEIVE: 'RECEIVE',
  PROCESS: 'PROCESS'
})

const OTEL_HEADERS = ['traceparent', 'tracestate', 'baggage']

const OTEL_TRACER_NAME = 'ml-kafka'

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
  OTEL_TRACER_NAME,
  stateList,
  kafkaBrokerStates
}
