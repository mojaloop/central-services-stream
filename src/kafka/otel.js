const { propagation, context, SpanKind, trace, SpanStatusCode } = require('@opentelemetry/api')
const { OTEL_HEADERS, SemConv } = require('../constants')

const tracer = trace.getTracer('kafka')

/* istanbul ignore next */
const startConsumerTracingSpan = (payload, consumerConfig = null, spanName = '', spanAttrs = null) => {
  const messages = Array.isArray(payload) ? payload : [payload]
  const { headers, topic } = messages[0]
  const otelHeaders = extractOtelHeaders(headers)

  const activeContext = Object.keys(otelHeaders).length
    ? propagation.extract(context.active(), otelHeaders)
    : context.active()

  // collect span links from other messages with distinct traceparents
  const links = []
  if (messages.length > 1) {
    const seenTraceparents = new Set([otelHeaders.traceparent])
    for (let i = 1; i < messages.length; i++) {
      const msgHeaders = extractOtelHeaders(messages[i].headers)
      if (msgHeaders.traceparent && !seenTraceparents.has(msgHeaders.traceparent)) {
        seenTraceparents.add(msgHeaders.traceparent)
        const linkedContext = propagation.extract(context.active(), msgHeaders)
        const linkedSpan = trace.getSpan(linkedContext)
        if (linkedSpan) {
          links.push({ context: linkedSpan.spanContext() })
        }
      }
    }
  }

  const span = tracer.startSpan(
    spanName || `RECEIVE:${topic}`,
    { kind: SpanKind.CONSUMER, links },
    activeContext
  )
  const spanCtx = trace.setSpan(activeContext, span)

  if (consumerConfig) span.setAttributes(makeConsumerAttributes(consumerConfig, topic, payload))
  if (spanAttrs) span.setAttributes(spanAttrs)

  return {
    span,
    topic,
    executeInsideSpanContext: async (fn, withSpanEnd = true, rethrowError = true) => context.with(
      spanCtx,
      () => executeAndSetSpanStatus(fn, span, withSpanEnd, rethrowError)
    )
  }
}

const executeAndSetSpanStatus = async (fn, span, withSpanEnd, rethrowError) => {
  try {
    const result = await fn()
    span.setStatus({ code: SpanStatusCode.OK })
    return result
  } catch (err) {
    span.setStatus({ code: SpanStatusCode.ERROR })
    span.recordException(err)
    if (rethrowError) throw err
  } finally {
    if (withSpanEnd) span.end()
  }
}

const makeConsumerAttributes = (config, topic, payload = null) => {
  const actualCount = Array.isArray(payload) ? payload.length : (payload ? 1 : null)
  return {
    [SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: actualCount || config.options.batchSize,
    [SemConv.ATTR_MESSAGING_CLIENT_ID]: config.rdkafkaConf['client.id'],
    [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: config.rdkafkaConf['group.id'],
    [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'receive',
    [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka',
    [SemConv.ATTR_SERVER_ADDRESS]: config.rdkafkaConf['metadata.broker.list']
  }
}

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
