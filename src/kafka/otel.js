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

const { trace, propagation, context, SpanKind, SpanStatusCode } = require('@opentelemetry/api')
const { SemConv, SpanPrefixes, OTEL_HEADERS, OTEL_TRACER_NAME } = require('../constants')
const { logger } = require('../lib/logger')

const tracer = trace.getTracer(OTEL_TRACER_NAME)

/**
 * @typedef {object} OtelHeaders
 * @prop {string} [traceparent]
 * @prop {string} [tracestate]
 * @prop {string} [baggage]
 */

/**
 * @typedef {Map<object, OtelHeaders>} MsgHeadersMap
 */

/* istanbul ignore next */
const startConsumerTracingSpan = (payload, consumerConfig = null, spanName = '', spanAttrs = null) => {
  const messages = Array.isArray(payload) ? payload : [payload]
  const isBatch = messages.length > 1

  const { primaryMsg, primaryHeaders, msgHeaders, receiveStartTime } = scanBatchForPrimary(messages)
  const { topic } = primaryMsg

  // Phase 2: parent context from primary message
  const activeContext = Object.keys(primaryHeaders).length
    ? propagation.extract(context.active(), primaryHeaders)
    : context.active()

  const { links, messageContexts } = buildBatchLinksAndContexts({
    isBatch, primaryMsg, primaryHeaders, msgHeaders, activeContext
  })

  // RECEIVE:<topic-name> span (measures broker transit, fire-and-forget)
  createReceiveSpan({
    spanName,
    topic,
    activeContext,
    links,
    startTime: receiveStartTime,
    consumerAttrs: makeConsumerAttributes(consumerConfig, topic, payload, SpanPrefixes.RECEIVE),
    spanAttrs
  })

  // PROCESS:<topic-name> span (measures handler execution, sibling of RECEIVE)
  const processSpan = tracer.startSpan(
    `${SpanPrefixes.PROCESS}:${topic}`,
    { kind: SpanKind.CONSUMER },
    activeContext
  )
  const processAttributes = {
    ...makeConsumerAttributes(consumerConfig, topic, payload, SpanPrefixes.PROCESS),
    ...spanAttrs
  }
  processSpan.setAttributes(processAttributes)
  const processSpanCtx = trace.setSpan(activeContext, processSpan)

  return {
    span: processSpan,
    topic,
    messageContexts: messageContexts?.size ? messageContexts : null,
    executeInsideSpanContext: async (fn, withSpanEnd = true, rethrowError = true) => context.with(
      processSpanCtx,
      () => executeAndSetSpanStatus(fn, processSpan, withSpanEnd, rethrowError, processAttributes)
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

const makeConsumerAttributes = (config, topic, payload = null, operationName = 'receive') => {
  if (!config || !topic) return {}

  const messages = Array.isArray(payload)
    ? payload
    : (payload ? [payload] : [])

  return {
    ...makeCommonKafkaAttributes(config, topic),
    ...makeMessageCountAttrs(messages),
    [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: config.rdkafkaConf['group.id'],
    [SemConv.ATTR_MESSAGING_OPERATION_NAME]: operationName?.toLowerCase()
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
  const spanName = `${SpanPrefixes.SEND}:${topicConf.topicName}`

  const executeFn = async (span) => { // think better naming
    const attributes = makeProducerAttributes(config, topicConf)
    span.setAttributes(attributes)
    const headers = injectTraceHeaders(customHeaders)

    return executeAndSetSpanStatus(
      () => produceFn(headers, attributes),
      span, true, true, attributes
    )
  }

  return tracer.startActiveSpan(
    spanName,
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

/**
 * Runs fn inside the OTel context of the original Kafka message.
 * Use in batch handlers to restore per-message trace context before producing.
 * Falls back to running fn directly if no context is found.
 *
 * @param {object} message - original Kafka message object from the consumed batch
 * @param {ConsumerCallbackMeta} meta - meta from consumer callback (3rd arg)
 * @param {Function} fn - async function to execute inside the message's OTel context
 * @returns {*} result of fn
 */
const withMessageContext = (message, meta, fn) => {
  const msgContext = meta?.messageContexts?.get(message)
  if (msgContext) return context.with(msgContext, fn)
  return fn()
}

/**
 * Scan batch messages to find the primary (sampled) message and extract OTel headers.
 *
 * @param {object[]} messages - array of Kafka message objects
 * @returns {{ primaryMsg: object, primaryHeaders: OtelHeaders, receiveStartTime: number, msgHeaders: MsgHeadersMap }}
 */
const scanBatchForPrimary = (messages) => {
  const msgHeaders = new Map()
  let primaryMsg = messages[0]
  let primaryHeaders = null

  for (const msg of messages) {
    const extracted = extractOtelHeaders(msg.headers)
    msgHeaders.set(msg, extracted)
    if (!primaryHeaders && isTraceSampled(extracted.traceparent)) {
      primaryMsg = msg
      primaryHeaders = extracted
    }
  }
  if (!primaryHeaders) primaryHeaders = msgHeaders.get(primaryMsg)

  const receiveStartTime = primaryMsg?.timestamp || Date.now()

  return {
    primaryMsg,
    primaryHeaders,
    receiveStartTime,
    msgHeaders
  }
}

/**
 * Build OTel span links and per-message context map for batch consumption.
 * Deduplicates links by traceparent to avoid redundant span references.
 *
 * @param {object} params
 * @param {boolean} params.isBatch - whether the payload contains multiple messages
 * @param {MsgHeadersMap} params.msgHeaders - map of messages to their extracted OTel headers
 * @param {object} params.primaryMsg - the primary (sampled) message from the batch
 * @param {OtelHeaders} params.primaryHeaders - OTel headers of the primary message
 * @param {import('@opentelemetry/api').Context} params.activeContext - parent OTel context
 * @returns {{ links: Array<{context: import('@opentelemetry/api').SpanContext}>, messageContexts: Map<object, import('@opentelemetry/api').Context> | null }}
 */
const buildBatchLinksAndContexts = ({
  isBatch, msgHeaders, primaryMsg, primaryHeaders, activeContext
}) => {
  if (!isBatch) return { links: [], messageContexts: null }

  const links = []
  const messageContexts = new Map()
  if (primaryHeaders.traceparent) messageContexts.set(primaryMsg, activeContext)

  const seenTraceparents = new Set([primaryHeaders.traceparent])

  for (const [msg, headers] of msgHeaders) {
    if (msg === primaryMsg || !headers.traceparent) continue
    const extractedContext = propagation.extract(context.active(), headers)
    messageContexts.set(msg, extractedContext)

    if (!seenTraceparents.has(headers.traceparent)) {
      seenTraceparents.add(headers.traceparent)
      const linkedSpan = trace.getSpan(extractedContext)
      if (linkedSpan) links.push({ context: linkedSpan.spanContext() })
    }
  }

  return { links, messageContexts }
}

const createReceiveSpan = ({
  spanName, topic, startTime, activeContext, links, consumerAttrs, spanAttrs
}) => {
  const span = tracer.startSpan(
    spanName || `${SpanPrefixes.RECEIVE}:${topic}`,
    { kind: SpanKind.CONSUMER, links, startTime },
    activeContext
  )
  span.setAttributes({ ...consumerAttrs, ...spanAttrs })
  span.setStatus({ code: SpanStatusCode.OK })
  span.end()
}

const isTraceSampled = (traceparent) => {
  if (!traceparent) return false
  const parts = traceparent.split('-')
  return parts.length === 4 && (parseInt(parts[3], 16) & 0x01) === 1
}

module.exports = {
  executeAndSetSpanStatus,
  extractOtelHeaders,
  injectTraceHeaders,
  makeConsumerAttributes,
  makeProducerAttributes,
  startConsumerTracingSpan,
  startProducerTracingSpan,
  withMessageContext
}
