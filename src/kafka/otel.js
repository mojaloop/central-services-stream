const { propagation, context, SpanKind, trace, SpanStatusCode } = require('@opentelemetry/api')
const { ATTR_SERVER_ADDRESS } = require('@opentelemetry/semantic-conventions')
const { OTEL_HEADERS, SemConv, SpanPrefixes } = require('../constants')
const { logger } = require('../lib/logger')

const tracer = trace.getTracer('ml-kafka')

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
    spanName || `${SpanPrefixes.RECEIVE}:${topic}`,
    { kind: SpanKind.CONSUMER, links },
    activeContext
  )
  const spanCtx = trace.setSpan(activeContext, span)

  const attributes = {
    ...(consumerConfig && makeConsumerAttributes(consumerConfig, topic, payload)),
    ...spanAttrs
  }
  span.setAttributes(attributes)

  return {
    span,
    topic,
    executeInsideSpanContext: async (fn, withSpanEnd = true, rethrowError = true) => context.with(
      spanCtx,
      () => executeAndSetSpanStatus(fn, span, withSpanEnd, rethrowError, attributes)
    )
  }
}

const executeAndSetSpanStatus = async (fn, span, withSpanEnd, rethrowError, spanAttrs = null) => {
  try {
    if (spanAttrs) logger.info('kafka span attributes: ', { attributes: spanAttrs })
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
  const messages = Array.isArray(payload) ? payload : (payload ? [payload] : [])

  return {
    [ATTR_SERVER_ADDRESS]: config.rdkafkaConf['metadata.broker.list'],
    [SemConv.ATTR_MESSAGING_CLIENT_ID]: config.rdkafkaConf['client.id'],
    [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: config.rdkafkaConf['group.id'],
    [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'consume',
    [SemConv.ATTR_MESSAGING_OPERATION_TYPE]: 'process',
    [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka',
    ...makeMessageCountAttrs(messages)
  }
}

const makeMessageCountAttrs = (messages) => {
  if (messages.length > 1) {
    return { [SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: messages.length }
  }
  if (messages.length === 1) {
    const msg = messages[0]
    return {
      ...(msg.partition != null && { [SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(msg.partition) }),
      ...(msg.offset != null && { [SemConv.ATTR_MESSAGING_KAFKA_OFFSET]: msg.offset }),
      ...(msg.key != null && { [SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: String(msg.key) })
    }
  }
  return {}
}

const makeProducerAttributes = (config, topicName) => ({
  [ATTR_SERVER_ADDRESS]: config.rdkafkaConf['metadata.broker.list'],
  [SemConv.ATTR_MESSAGING_CLIENT_ID]: config.rdkafkaConf['client.id'],
  [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topicName,
  [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'send',
  [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka'
})

const injectTraceHeaders = (customHeaders = []) => {
  const tracingContext = {}
  propagation.inject(context.active(), tracingContext)
  return [
    ...customHeaders,
    ...Object.entries(tracingContext).map(([k, v]) => ({ [k]: v }))
  ]
}

const startProducerTracingSpan = async (topicName, config, customHeaders, produceFn) => {
  return tracer.startActiveSpan(
    `${SpanPrefixes.SEND}:${topicName}`,
    { kind: SpanKind.PRODUCER },
    async (span) => {
      const attributes = makeProducerAttributes(config, topicName)
      span.setAttributes(attributes)
      const headers = injectTraceHeaders(customHeaders)
      return executeAndSetSpanStatus(
        () => produceFn(headers),
        span, true, true, attributes
      )
    }
  )
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
  injectTraceHeaders,
  makeConsumerAttributes,
  makeProducerAttributes,
  startConsumerTracingSpan,
  startProducerTracingSpan
}
