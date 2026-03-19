/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Gates Foundation organization for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.
 * Gates Foundation
 - Name Surname <name.surname@gatesfoundation.com>

 * Eugen Klymniuk <eugen.klymniuk@infitx.com>
 --------------
 **********/

const Test = require('tapes')(require('tape'))
const sinon = require('sinon')
const { trace, SpanStatusCode, propagation, context } = require('@opentelemetry/api')

const otel = require('#src/kafka/otel')
const { SemConv, SpanPrefixes } = require('#src/constants')
const { tryCatchEndTest } = require('#test/utils')
const mocks = require('../mocks')

const spanStubProps = () => ({
  setAttribute: sinon.stub(),
  setStatus: sinon.stub(),
  setAttributes: sinon.stub(),
  recordException: sinon.stub(),
  end: sinon.stub(),
  spanContext: sinon.stub().returns({ traceId: 'a'.repeat(32), spanId: 'b'.repeat(16), traceFlags: 0 })
})
const createSpanStub = () => Object.freeze(spanStubProps())

Test('otel Tests -->', (otelSuite) => {
  otelSuite.test('should return empty object if kafkaHeaders is not array with elements', tryCatchEndTest((assert) => {
    const kafkaHeaders = null
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    assert.deepEqual(otelHeaders, {}, 'returns empty object for null headers')
  }))

  otelSuite.test('should extract OTel headers from array with one-with-all headers element', tryCatchEndTest((assert) => {
    const kafkaHeaders = [
      {
        'content-type': 'application/json',
        traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
        tracestate: 'congo=t61rcWkgMzE',
        baggage: 'key1=value1,key2=value2'
      }
    ]
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    assert.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    assert.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    assert.ok(otelHeaders.baggage, 'baggage header is extracted')
    assert.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  otelSuite.test('should extract OTel headers from array with several headers elements', tryCatchEndTest((assert) => {
    const kafkaHeaders = [
      { 'content-type': 'application/json' },
      { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
      { tracestate: 'congo=t61rcWkgMzE' },
      { baggage: 'key1=value1,key2=value2' }
    ]
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    assert.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    assert.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    assert.ok(otelHeaders.baggage, 'baggage header is extracted')
    assert.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  otelSuite.test('executeAndSetSpanStatus Tests -->', (executeMethodTests) => {
    executeMethodTests.test('should set error status and record exception in case if fn failed', tryCatchEndTest(async (assert) => {
      const error = new Error('Test error')
      const fn = () => { throw error }
      const span = createSpanStub()
      const withSpanEnd = false
      const rethrowError = false

      await otel.executeAndSetSpanStatus(fn, span, withSpanEnd, rethrowError)
      assert.true(span.setStatus.calledWith({ code: SpanStatusCode.ERROR }), 'span.setStatus() was called with error code')
      assert.true(span.recordException.calledWith(error), 'span.recordException() was called with error')
      assert.true(span.setAttribute.calledWith(SemConv.ATTR_ERROR_TYPE, 'Error'), 'error.type set to err.name')
    }))

    executeMethodTests.test('should prefer err.code for error.type attribute', tryCatchEndTest(async (assert) => {
      const error = new Error('kafka transport failed')
      error.code = 'ERR__TRANSPORT'
      const fn = () => { throw error }
      const span = createSpanStub()

      await otel.executeAndSetSpanStatus(fn, span, false, false)
      assert.true(span.setAttribute.calledWith(SemConv.ATTR_ERROR_TYPE, 'ERR__TRANSPORT'), 'uses err.code')
    }))

    executeMethodTests.test('should call span.end() if withSpanEnd === true', tryCatchEndTest(async (assert) => {
      const fn = () => {}
      const span = createSpanStub()
      const withSpanEnd = true

      await otel.executeAndSetSpanStatus(fn, span, withSpanEnd)
      assert.true(span.end.calledOnce, 'span.end() was called')
    }))

    executeMethodTests.end()
  })

  otelSuite.test('makeConsumerAttributes Tests -->', (attrTests) => {
    const baseConfig = mocks.kafkaConfigMockDto({
      options: { batchSize: 5 },
      rdkafkaConf: mocks.rdkafkaConfMockDto({
        clientId: 'test-client',
        groupId: 'test-group'
      })
    })

    attrTests.test('should use actual count from array payload', tryCatchEndTest((assert) => {
      const payload = [{ value: 1 }, { value: 2 }, { value: 3 }]
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT], 3, 'count should be 3 for array of 3')
    }))

    attrTests.test('should not set batch count for single message payload', tryCatchEndTest((assert) => {
      const payload = { value: 1, partition: 0, offset: 42, key: 'msg-key' }
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT], undefined, 'batch count should not be set for single message')
    }))

    attrTests.test('should not set batch count when no payload', tryCatchEndTest((assert) => {
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT], undefined, 'batch count should not be set for empty payload')
    }))

    attrTests.test('should set all standard consumer attributes', tryCatchEndTest((assert) => {
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_CLIENT_ID], 'test-client', 'client.id is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME], 'test-group', 'group.id is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_NAME], 'test-topic', 'destination name is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_OPERATION_NAME], 'receive', 'operation name is receive')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_SYSTEM], 'kafka', 'messaging system is kafka')
      assert.equal(attrs[SemConv.ATTR_SERVER_ADDRESS], 'localhost:9092', 'server address is set')
    }))

    attrTests.test('should set Kafka-specific attrs for single message', tryCatchEndTest((assert) => {
      const payload = { value: 'test', partition: 2, offset: 99, key: 'order-123' }
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], '2', 'partition as string')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_OFFSET], 99, 'offset preserved as number')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], 'order-123', 'key as string')
    }))

    attrTests.test('should set Kafka-specific attrs when partition is 0 and offset is 0', tryCatchEndTest((assert) => {
      const payload = { value: 'test', partition: 0, offset: 0, key: null }
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], '0', 'partition 0 is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_OFFSET], 0, 'offset 0 is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], undefined, 'null key is not set')
    }))

    attrTests.test('should not set Kafka-specific attrs for batch payload', tryCatchEndTest((assert) => {
      const payload = [
        { value: 'a', partition: 0, offset: 10, key: 'k1' },
        { value: 'b', partition: 0, offset: 11, key: 'k2' },
        { value: 'c', partition: 1, offset: 5, key: 'k3' }
      ]
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], undefined, 'partition not set for batch')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_OFFSET], undefined, 'offset not set for batch')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], undefined, 'key not set for batch')
    }))

    attrTests.end()
  })

  otelSuite.test('makeProducerAttributes Tests -->', (attrTests) => {
    const baseConfig = mocks.kafkaConfigMockDto()

    attrTests.test('should set all standard producer attributes', tryCatchEndTest((assert) => {
      const topicConf = { topicName: 'test-topic' }
      const attrs = otel.makeProducerAttributes(baseConfig, topicConf)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_CLIENT_ID], 'producer-client', 'client.id is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_NAME], 'test-topic', 'destination name is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_OPERATION_NAME], 'send', 'operation name is send')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_SYSTEM], 'kafka', 'messaging system is kafka')
      assert.equal(attrs[SemConv.ATTR_SERVER_ADDRESS], 'localhost:9092', 'server address is set')
    }))

    attrTests.test('should not include consumer-specific attributes', tryCatchEndTest((assert) => {
      const topicConf = { topicName: 'test-topic' }
      const attrs = otel.makeProducerAttributes(baseConfig, topicConf)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME], undefined, 'no consumer group')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT], undefined, 'no batch count')
    }))

    attrTests.test('should set partition and key when available', tryCatchEndTest((assert) => {
      const topicConf = { topicName: 'test-topic', partition: 3, key: 'order-456' }
      const attrs = otel.makeProducerAttributes(baseConfig, topicConf)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], '3', 'partition as string')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], 'order-456', 'key as string')
    }))

    attrTests.test('should set partition when it is 0', tryCatchEndTest((assert) => {
      const topicConf = { topicName: 'test-topic', partition: 0 }
      const attrs = otel.makeProducerAttributes(baseConfig, topicConf)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], '0', 'partition 0 is set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], undefined, 'key not set when absent')
    }))

    attrTests.test('should not set partition and key when not provided', tryCatchEndTest((assert) => {
      const topicConf = { topicName: 'test-topic' }
      const attrs = otel.makeProducerAttributes(baseConfig, topicConf)
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], undefined, 'partition not set')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], undefined, 'key not set')
    }))

    attrTests.end()
  })

  otelSuite.test('injectTraceHeaders Tests -->', (headerTests) => {
    headerTests.test('should return custom headers when no trace context is active', tryCatchEndTest((assert) => {
      const customHeaders = [{ 'x-custom': 'value' }]
      const headers = otel.injectTraceHeaders(customHeaders)
      assert.ok(Array.isArray(headers), 'returns array')
      assert.ok(headers.some(h => h['x-custom'] === 'value'), 'custom header preserved')
    }))

    headerTests.test('should return empty array when called with no args and no trace context', tryCatchEndTest((assert) => {
      const headers = otel.injectTraceHeaders()
      assert.ok(Array.isArray(headers), 'returns array')
    }))

    headerTests.end()
  })

  otelSuite.test('startProducerTracingSpan Tests -->', (spanTests) => {
    const config = mocks.kafkaConfigMockDto()

    spanTests.test('should call produceFn with headers and return its result', tryCatchEndTest(async (assert) => {
      const topicConf = { topicName: 'test-topic' }
      const produceFn = sinon.stub().resolves('produce-result')
      const result = await otel.startProducerTracingSpan(config, topicConf, [], produceFn)
      assert.equal(result, 'produce-result', 'returns produceFn result')
      assert.true(produceFn.calledOnce, 'produceFn called once')
      assert.ok(Array.isArray(produceFn.firstCall.args[0]), 'produceFn called with headers array')
    }))

    spanTests.test('should propagate error from produceFn', tryCatchEndTest(async (assert) => {
      const topicConf = { topicName: 'test-topic' }
      const error = new Error('produce failed')
      const produceFn = sinon.stub().rejects(error)
      try {
        await otel.startProducerTracingSpan(config, topicConf, [], produceFn)
        assert.fail('should have thrown')
      } catch (err) {
        assert.equal(err.message, 'produce failed', 'error propagated')
      }
    }))

    spanTests.test('should call produceFn with headers and producer attributes', tryCatchEndTest(async (assert) => {
      const topicConf = { topicName: 'test-topic', partition: 2, key: 'msg-key' }
      const produceFn = sinon.stub().resolves('ok')
      await otel.startProducerTracingSpan(config, topicConf, [], produceFn)
      const [headers, attrs] = produceFn.firstCall.args
      assert.ok(Array.isArray(headers), 'first arg is headers array')
      assert.ok(attrs, 'second arg (attributes) is provided')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_SYSTEM], 'kafka', 'attributes contain messaging.system')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_NAME], 'test-topic', 'attributes contain topic name')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_OPERATION_NAME], 'send', 'attributes contain operation name')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], '2', 'attributes contain partition')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], 'msg-key', 'attributes contain message key')
    }))

    spanTests.test('should call produceFn with attributes without optional fields when partition and key are null', tryCatchEndTest(async (assert) => {
      const topicConf = { topicName: 'test-topic' }
      const produceFn = sinon.stub().resolves('ok')
      await otel.startProducerTracingSpan(config, topicConf, [], produceFn)
      const attrs = produceFn.firstCall.args[1]
      assert.ok(attrs, 'attributes object is provided')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_DESTINATION_PARTITION_ID], undefined, 'no partition when not specified')
      assert.equal(attrs[SemConv.ATTR_MESSAGING_KAFKA_MESSAGE_KEY], undefined, 'no key when not specified')
    }))

    spanTests.test('should merge custom headers with trace headers', tryCatchEndTest(async (assert) => {
      const topicConf = { topicName: 'test-topic' }
      const customHeaders = [{ 'x-request-id': 'abc' }]
      const produceFn = sinon.stub().resolves(true)

      await otel.startProducerTracingSpan(config, topicConf, customHeaders, produceFn)
      const headers = produceFn.firstCall.args[0]
      assert.ok(headers.some(h => h['x-request-id'] === 'abc'), 'custom header included')
    }))

    spanTests.end()
  })

  const batchConsumerConfig = mocks.kafkaConfigMockDto({
    options: { batchSize: 5 },
    rdkafkaConf: mocks.rdkafkaConfMockDto({ clientId: 'test-client', groupId: 'test-group' })
  })

  const makeMsg = (traceId, spanId, traceFlags, topic = 'test-topic') => ({
    topic,
    headers: [
      { traceparent: `00-${traceId}-${spanId}-${traceFlags}` }
    ],
    value: 'test'
  })

  const TRACE_A = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  const TRACE_B = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
  const TRACE_C = 'cccccccccccccccccccccccccccccccc'
  const SPAN_1 = 'a1a1a1a1a1a1a1a1'
  const SPAN_2 = 'b2b2b2b2b2b2b2b2'
  const SPAN_3 = 'c3c3c3c3c3c3c3c3'

  otelSuite.test('startConsumerTracingSpan batch tracing Tests -->', (batchTests) => {
    batchTests.test('should select first sampled message as primary when messages[0] is unsampled', tryCatchEndTest((assert) => {
      const batch = [
        makeMsg(TRACE_A, SPAN_1, '00', 'topic-unsampled-0'),
        makeMsg(TRACE_B, SPAN_2, '00', 'topic-unsampled-1'),
        makeMsg(TRACE_C, SPAN_3, '01', 'topic-sampled-2')
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.topic, 'topic-sampled-2', 'primary message is the first sampled one at index 2')
    }))

    batchTests.test('should fall back to messages[0] when no message is sampled', tryCatchEndTest((assert) => {
      const batch = [
        makeMsg(TRACE_A, SPAN_1, '00', 'topic-first'),
        makeMsg(TRACE_B, SPAN_2, '00', 'topic-second'),
        makeMsg(TRACE_C, SPAN_3, '00', 'topic-third')
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.topic, 'topic-first', 'falls back to messages[0] when none is sampled')
    }))

    batchTests.test('should use single message traceparent without scanning', tryCatchEndTest((assert) => {
      const msg = makeMsg(TRACE_A, SPAN_1, '00', 'topic-single')
      const result = otel.startConsumerTracingSpan(msg, batchConsumerConfig)
      assert.equal(result.topic, 'topic-single', 'single message used directly without batch scan')
    }))

    batchTests.test('should select sampled message as primary and process all messages including messages[0]', tryCatchEndTest((assert) => {
      // Stub propagation.extract to track which messages' headers are extracted
      const extractedTraceparents = []
      const originalExtract = propagation.extract.bind(propagation)
      const extractStub = sinon.stub(propagation, 'extract').callsFake((ctx, headers) => {
        if (headers.traceparent) extractedTraceparents.push(headers.traceparent)
        return originalExtract(ctx, headers)
      })

      try {
        const batch = [
          makeMsg(TRACE_A, SPAN_1, '00', 'topic-unsampled'),
          makeMsg(TRACE_B, SPAN_2, '01', 'topic-sampled')
        ]
        const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)

        assert.equal(result.topic, 'topic-sampled', 'sampled message at index 1 is primary')
        const tpA = `00-${TRACE_A}-${SPAN_1}-00`
        assert.ok(extractedTraceparents.includes(tpA), 'messages[0] traceparent is extracted for span links')
      } finally {
        extractStub.restore()
      }
    }))

    batchTests.test('should treat trace-flags with bit 0 set as sampled (e.g., 03)', tryCatchEndTest((assert) => {
      const batch = [
        makeMsg(TRACE_A, SPAN_1, '00', 'topic-unsampled'),
        makeMsg(TRACE_B, SPAN_2, '03', 'topic-sampled-03')
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.topic, 'topic-sampled-03', 'trace-flags 03 treated as sampled (bit 0 set)')
    }))

    batchTests.test('should skip messages with missing traceparent during scan', tryCatchEndTest((assert) => {
      const msgNoHeaders = { topic: 'topic-no-headers', headers: [], value: 'test' }
      const msgEmptyTp = { topic: 'topic-empty-tp', headers: [{ traceparent: '' }], value: 'test' }
      const msgSampled = makeMsg(TRACE_C, SPAN_3, '01', 'topic-sampled')
      const batch = [msgNoHeaders, msgEmptyTp, msgSampled]

      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.topic, 'topic-sampled', 'skips messages with missing/empty traceparent')
    }))

    batchTests.test('should fall back to messages[0] when all messages have missing traceparent', tryCatchEndTest((assert) => {
      const batch = [
        { topic: 'topic-first', headers: [], value: 'a' },
        { topic: 'topic-second', headers: [{ traceparent: '' }], value: 'b' },
        { topic: 'topic-third', headers: null, value: 'c' }
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.topic, 'topic-first', 'falls back to messages[0] when all traceparents missing')
    }))

    batchTests.end()
  })

  otelSuite.test('messageContexts Map Tests -->', (msgCtxTests) => {
    msgCtxTests.test('should return messageContexts as null for single message', tryCatchEndTest((assert) => {
      const msg = makeMsg(TRACE_A, SPAN_1, '01')
      const result = otel.startConsumerTracingSpan(msg, batchConsumerConfig)
      assert.equal(result.messageContexts, null, 'messageContexts is null for single message (not array)')
    }))

    msgCtxTests.test('should return messageContexts as null for array of 1', tryCatchEndTest((assert) => {
      const batch = [makeMsg(TRACE_A, SPAN_1, '01')]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.messageContexts, null, 'messageContexts is null for array of 1')
    }))

    msgCtxTests.test('should return messageContexts Map for batch of 3 messages', tryCatchEndTest((assert) => {
      const batch = [
        makeMsg(TRACE_A, SPAN_1, '01'),
        makeMsg(TRACE_B, SPAN_2, '01'),
        makeMsg(TRACE_C, SPAN_3, '01')
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.ok(result.messageContexts instanceof Map, 'messageContexts is a Map')
      assert.equal(result.messageContexts.size, 3, 'Map has 3 entries for 3 messages')
      assert.ok(result.messageContexts.has(batch[0]), 'Map contains batch[0]')
      assert.ok(result.messageContexts.has(batch[1]), 'Map contains batch[1]')
      assert.ok(result.messageContexts.has(batch[2]), 'Map contains batch[2]')
    }))

    msgCtxTests.test('should include primaryMsg in messageContexts map', tryCatchEndTest((assert) => {
      const batch = [
        makeMsg(TRACE_A, SPAN_1, '00'),
        makeMsg(TRACE_B, SPAN_2, '00'),
        makeMsg(TRACE_C, SPAN_3, '01')
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.ok(result.messageContexts instanceof Map, 'messageContexts is a Map')
      assert.ok(result.messageContexts.has(batch[2]), 'sampled message at index 2 is in the Map')
    }))

    msgCtxTests.test('should not include messages without OTel headers in map', tryCatchEndTest((assert) => {
      const msgNoHeaders = { topic: 'topic-no-headers', headers: [], value: 'test' }
      const msgWithHeaders = makeMsg(TRACE_A, SPAN_1, '01')
      const batch = [msgWithHeaders, msgNoHeaders]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.ok(result.messageContexts instanceof Map, 'messageContexts is a Map')
      assert.ok(result.messageContexts.has(msgWithHeaders), 'message with headers is in Map')
      assert.false(result.messageContexts.has(msgNoHeaders), 'message without headers is NOT in Map')
    }))

    msgCtxTests.test('should not include primaryMsg (messages[0]) without OTel headers in map', tryCatchEndTest((assert) => {
      const msgNoHeaders = { topic: 'topic-no-headers', headers: [], value: 'first' }
      const msgWithHeaders = makeMsg(TRACE_A, SPAN_1, '01')
      const batch = [msgNoHeaders, msgWithHeaders]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.ok(result.messageContexts instanceof Map, 'messageContexts is a Map')
      assert.ok(result.messageContexts.has(msgWithHeaders), 'message with headers is in Map')
      assert.false(result.messageContexts.has(msgNoHeaders), 'primaryMsg without headers is NOT in Map')
      assert.equal(result.messageContexts.size, 1, 'Map has only 1 entry')
    }))

    msgCtxTests.test('should return messageContexts as null when no messages have OTel headers', tryCatchEndTest((assert) => {
      const batch = [
        { topic: 'topic-1', headers: [], value: 'a' },
        { topic: 'topic-2', headers: null, value: 'b' },
        { topic: 'topic-3', headers: [{ 'content-type': 'application/json' }], value: 'c' }
      ]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.equal(result.messageContexts, null, 'messageContexts is null when no messages have OTel headers')
    }))

    msgCtxTests.test('should store separate entries for messages with same traceparent', tryCatchEndTest((assert) => {
      const msg1 = makeMsg(TRACE_A, SPAN_1, '01')
      const msg2 = makeMsg(TRACE_A, SPAN_1, '01')
      const batch = [msg1, msg2]
      const result = otel.startConsumerTracingSpan(batch, batchConsumerConfig)
      assert.ok(result.messageContexts instanceof Map, 'messageContexts is a Map')
      assert.equal(result.messageContexts.size, 2, 'both messages in Map despite same traceparent')
      assert.ok(result.messageContexts.has(msg1), 'msg1 is in Map')
      assert.ok(result.messageContexts.has(msg2), 'msg2 is in Map')
    }))

    msgCtxTests.end()
  })

  otelSuite.test('withMessageContext Tests -->', (withCtxTests) => {
    withCtxTests.test('should run fn in message context when found in map', tryCatchEndTest((assert) => {
      const mockContext = { _test: 'mock-context' }
      const messageContexts = new Map()
      const message = { topic: 'test', value: 'data' }
      messageContexts.set(message, mockContext)

      const contextWithStub = sinon.stub(context, 'with').callsFake((_ctx, fn) => fn())
      try {
        const fn = sinon.stub().returns('result')
        const meta = { messageContexts }
        const result = otel.withMessageContext(message, meta, fn)
        assert.true(contextWithStub.calledOnce, 'context.with called once')
        assert.equal(contextWithStub.firstCall.args[0], mockContext, 'context.with called with correct context')
        assert.equal(result, 'result', 'returns fn result')
      } finally {
        contextWithStub.restore()
      }
    }))

    withCtxTests.test('should fall back to fn() when message not in map', tryCatchEndTest((assert) => {
      const messageContexts = new Map()
      const message = { topic: 'test', value: 'data' }
      const otherMessage = { topic: 'other', value: 'other' }
      messageContexts.set(otherMessage, { _test: 'other-context' })

      const contextWithStub = sinon.stub(context, 'with')
      try {
        const fn = sinon.stub().returns('direct-result')
        const meta = { messageContexts }
        const result = otel.withMessageContext(message, meta, fn)
        assert.true(fn.calledOnce, 'fn called once')
        assert.false(contextWithStub.called, 'context.with not called')
        assert.equal(result, 'direct-result', 'returns fn result directly')
      } finally {
        contextWithStub.restore()
      }
    }))

    withCtxTests.test('should fall back when meta.messageContexts is null', tryCatchEndTest((assert) => {
      const contextWithStub = sinon.stub(context, 'with')
      try {
        const fn = sinon.stub().returns('fallback-result')
        const meta = { batchId: 'p0.0-p0.1', batchSize: 2 }
        const message = { topic: 'test', value: 'data' }
        const result = otel.withMessageContext(message, meta, fn)
        assert.true(fn.calledOnce, 'fn called once')
        assert.false(contextWithStub.called, 'context.with not called')
        assert.equal(result, 'fallback-result', 'returns fn result directly')
      } finally {
        contextWithStub.restore()
      }
    }))

    withCtxTests.test('should fall back when meta is null/undefined', tryCatchEndTest((assert) => {
      const contextWithStub = sinon.stub(context, 'with')
      try {
        const fn = sinon.stub().returns('null-meta-result')
        const result = otel.withMessageContext({ topic: 'test' }, null, fn)
        assert.true(fn.calledOnce, 'fn called once with null meta')
        assert.false(contextWithStub.called, 'context.with not called with null meta')
        assert.equal(result, 'null-meta-result', 'returns fn result directly')

        const resultUndefined = otel.withMessageContext({ topic: 'test' }, undefined, fn)
        assert.equal(fn.callCount, 2, 'fn called again with undefined meta')
        assert.equal(resultUndefined, 'null-meta-result', 'returns fn result for undefined meta')
      } finally {
        contextWithStub.restore()
      }
    }))

    withCtxTests.end()
  })

  otelSuite.test('RECEIVE/PROCESS span split Tests -->', (splitTests) => {
    const tracerProto = Object.getPrototypeOf(trace.getTracer('ml-kafka'))

    const BASE_TIMESTAMP = 1_700_000_000_000

    splitTests.test('should use message timestamp as RECEIVE startTime and create both spans eagerly', tryCatchEndTest((assert) => {
      const startSpanStub = sinon.stub(tracerProto, 'startSpan').callThrough()
      try {
        const msg = {
          topic: 'test-topic',
          headers: [],
          value: 'test',
          timestamp: BASE_TIMESTAMP
        }
        otel.startConsumerTracingSpan(msg, batchConsumerConfig)
        assert.equal(startSpanStub.callCount, 2, 'both RECEIVE and PROCESS spans created eagerly')

        const receiveOptions = startSpanStub.firstCall.args[1]
        assert.equal(receiveOptions.startTime, BASE_TIMESTAMP, 'RECEIVE span uses message timestamp as startTime')

        const processSpanName = startSpanStub.secondCall.args[0]
        assert.equal(processSpanName, `${SpanPrefixes.PROCESS}:test-topic`, 'PROCESS span name is PROCESS:{topic}')
        const processSpanParentCtx = startSpanStub.secondCall.args[2]
        assert.ok(processSpanParentCtx, 'PROCESS span receives parent context (receiveSpanCtx)')
      } finally {
        startSpanStub.restore()
      }
    }))

    splitTests.test('should return processSpan as span property', tryCatchEndTest((assert) => {
      const receiveSpanStub = spanStubProps()
      const processSpanStub = spanStubProps()

      const startSpanStub = sinon.stub(tracerProto, 'startSpan')
        .onFirstCall().returns(receiveSpanStub)
        .onSecondCall().returns(processSpanStub)
      try {
        const msg = { topic: 'test-topic', headers: [], value: 'test', timestamp: BASE_TIMESTAMP }
        const result = otel.startConsumerTracingSpan(msg, batchConsumerConfig)
        assert.equal(result.span, processSpanStub, 'span property is the PROCESS span, not RECEIVE')
      } finally {
        startSpanStub.restore()
      }
    }))

    splitTests.test('should end RECEIVE eagerly and end PROCESS after handler', tryCatchEndTest(async (assert) => {
      const callOrder = []
      const receiveSpanStub = spanStubProps()
      receiveSpanStub.end = sinon.stub().callsFake(() => callOrder.push('receive-end'))
      const processSpanStub = spanStubProps()
      processSpanStub.end = sinon.stub().callsFake(() => callOrder.push('process-end'))

      const startSpanStub = sinon.stub(tracerProto, 'startSpan')
        .onFirstCall().returns(receiveSpanStub)
        .onSecondCall().returns(processSpanStub)
      try {
        const msg = { topic: 'test-topic', headers: [], value: 'test', timestamp: BASE_TIMESTAMP }
        const result = otel.startConsumerTracingSpan(msg, batchConsumerConfig)
        assert.deepEqual(callOrder, ['receive-end'], 'RECEIVE ends eagerly during startConsumerTracingSpan')

        const handler = () => { callOrder.push('handler') }
        await result.executeInsideSpanContext(handler)
        assert.deepEqual(callOrder, ['receive-end', 'handler', 'process-end'], 'handler runs then PROCESS ends')
      } finally {
        startSpanStub.restore()
      }
    }))

    splitTests.test('should run handler inside PROCESS span context', tryCatchEndTest(async (assert) => {
      const startSpanStub = sinon.stub(tracerProto, 'startSpan').callThrough()
      try {
        const msg = { topic: 'test-topic', headers: [], value: 'test', timestamp: BASE_TIMESTAMP }
        const result = otel.startConsumerTracingSpan(msg, batchConsumerConfig)

        await result.executeInsideSpanContext(() => 'ok')
        assert.equal(startSpanStub.callCount, 2, 'two spans created: RECEIVE and PROCESS')
        const processSpanName = startSpanStub.secondCall.args[0]
        assert.equal(processSpanName, `${SpanPrefixes.PROCESS}:test-topic`, 'handler runs inside PROCESS context')
      } finally {
        startSpanStub.restore()
      }
    }))

    splitTests.test('should use primary message timestamp as RECEIVE startTime', tryCatchEndTest((assert) => {
      const startSpanStub = sinon.stub(tracerProto, 'startSpan').callThrough()
      try {
        const batch = [
          { topic: 'test-topic', headers: [], value: 'a', timestamp: BASE_TIMESTAMP + 2_000 },
          { topic: 'test-topic', headers: [], value: 'b', timestamp: BASE_TIMESTAMP + 1_000 },
          { topic: 'test-topic', headers: [], value: 'c', timestamp: BASE_TIMESTAMP + 3_000 }
        ]
        otel.startConsumerTracingSpan(batch, batchConsumerConfig)
        const spanOptions = startSpanStub.firstCall.args[1]
        assert.equal(spanOptions.startTime, BASE_TIMESTAMP + 2_000, 'RECEIVE span uses primary message timestamp, not earliest batch timestamp')
      } finally {
        startSpanStub.restore()
      }
    }))

    splitTests.end()
  })

  otelSuite.end()
})
