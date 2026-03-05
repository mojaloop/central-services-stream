/*****
 License
 --------------
 Copyright © 2020-2026 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Mojaloop Foundation
 * Eugen Klymniuk <eugen.klymniuk@infitx.com>

 --------------
 ******/

const { propagation, context, SpanKind, trace, SpanStatusCode } = require('@opentelemetry/api')
const { SemConv, SpanPrefixes, OTEL_HEADERS } = require('../constants')
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
    if (spanAttrs) logger.verbose('kafka span attributes: ', { attributes: spanAttrs })
    const result = await fn()
    span.setStatus({ code: SpanStatusCode.OK })
    return result
  } catch (err) {
    span.setStatus({ code: SpanStatusCode.ERROR })
    span.recordException(err)
    span.setAttribute(SemConv.ATTR_ERROR_TYPE, err?.code || err?.name || 'UnknownError')
    if (rethrowError) throw err
  } finally {
    if (withSpanEnd) span.end()
  }
}

const makeConsumerAttributes = (config, topic, payload = null) => {
  const messages = Array.isArray(payload) ? payload : (payload ? [payload] : [])

  return {
    ...makeCommonKafkaAttributes(config, topic),
    ...makeMessageCountAttrs(messages),
    [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: config.rdkafkaConf['group.id'],
    [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'receive'
  }
}

const makeProducerAttributes = (config, topicConf) => ({
  ...makeCommonKafkaAttributes(config, topicConf.topicName),
  [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'send',
  ...(topicConf.partition != null && {
    [SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(topicConf.partition)
  }),
  ...(topicConf.key != null && {
    [SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: String(topicConf.key)
  })
})

const makeCommonKafkaAttributes = (config, topicName) => ({
  [SemConv.ATTR_SERVER_ADDRESS]: config.rdkafkaConf['metadata.broker.list'],
  [SemConv.ATTR_MESSAGING_CLIENT_ID]: config.rdkafkaConf['client.id'],
  [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topicName,
  [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka'
})

const makeMessageCountAttrs = (messages = []) => {
  if (messages.length > 1) {
    return {
      [SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: messages.length
    }
  }
  if (messages.length === 1) {
    const msg = messages[0]
    return {
      ...(msg.partition != null && {
        [SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(msg.partition)
      }),
      ...(msg.offset != null && {
        [SemConv.ATTR_MESSAGING_KAFKA_OFFSET]: msg.offset
      }),
      ...(msg.key != null && {
        [SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: String(msg.key)
      })
    }
  }
  return {}
}

const injectTraceHeaders = (customHeaders = []) => {
  const tracingContext = {}
  propagation.inject(context.active(), tracingContext)
  return [
    ...customHeaders,
    ...Object.entries(tracingContext).map(([k, v]) => ({ [k]: v }))
  ]
}

const startProducerTracingSpan = async (config, topicConf, customHeaders, produceFn) => {
  const executeFn = async (span) => { // think better naming
    const attributes = makeProducerAttributes(config, topicConf)
    span.setAttributes(attributes)
    const headers = injectTraceHeaders(customHeaders)
    return executeAndSetSpanStatus(
      () => produceFn(headers),
      span, true, true, attributes
    )
  }

  return tracer.startActiveSpan(
    `${SpanPrefixes.SEND}:${topicConf.topicName}`,
    { kind: SpanKind.PRODUCER },
    executeFn
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
