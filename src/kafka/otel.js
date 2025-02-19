const { propagation, context, SpanKind, trace, SpanStatusCode } = require('@opentelemetry/api')
const { OTEL_HEADERS, SemConv } = require('../constants')

const tracer = trace.getTracer('kafka')

/* istanbul ignore next */
const startConsumerTracingSpan = (payload, consumerConfig = null, spanName = '', spanAttrs = null) => {
  // think, how to start tracing span if payload has more than 1 message? (each message in the batch should have its own span?)
  const { headers, topic } = Array.isArray(payload)
    ? payload[0]
    : payload
  const otelHeaders = extractOtelHeaders(headers)

  const activeContext = Object.keys(otelHeaders).length
    ? propagation.extract(context.active(), otelHeaders)
    : context.active()

  const span = tracer.startSpan(spanName || `RECEIVE:${topic}`, { kind: SpanKind.CONSUMER }, activeContext)
  const spanCtx = trace.setSpan(activeContext, span)

  if (consumerConfig) span.setAttributes(makeConsumerAttributes(consumerConfig, topic))
  if (spanAttrs) span.setAttributes(spanAttrs)

  return {
    span,
    topic,
    executeInsideSpanContext: async (fn, withSpanEnd = true) => context.with(
      spanCtx,
      () => executeAndSetSpanStatus(fn, span, withSpanEnd)
    )
  }
}

const executeAndSetSpanStatus = async (fn, span, withSpanEnd) => {
  try {
    const result = await fn()
    span.setStatus({ code: SpanStatusCode.OK })
    return result
  } catch (err) {
    span.setStatus({ code: SpanStatusCode.ERROR })
    span.recordException(err)
  } finally {
    if (withSpanEnd) span.end()
  }
}

const makeConsumerAttributes = (config, topic) => ({
  [SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: config.options.batchSize,
  [SemConv.ATTR_MESSAGING_CLIENT_ID]: config.rdkafkaConf['client.id'],
  [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: config.rdkafkaConf['group.id'],
  [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topic,
  [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'receive',
  [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka',
  [SemConv.ATTR_SERVER_ADDRESS]: config.rdkafkaConf['metadata.broker.list']
  // think, if we need more attributes
})

const extractOtelHeaders = (headers) =>
  (!Array.isArray(headers) || headers.length === 0)
    ? {}
    : headers.reduce((acc, header) => {
      Object.entries(header).forEach(([key, value]) => {
        if (OTEL_HEADERS.includes(key)) acc[key] = value?.toString()
      })
      return acc
    }, {})

module.exports = {
  executeAndSetSpanStatus,
  extractOtelHeaders,
  makeConsumerAttributes,
  startConsumerTracingSpan
}
