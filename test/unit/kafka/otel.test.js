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
const { SpanStatusCode } = require('@opentelemetry/api')

const otel = require('#src/kafka/otel')
const { SemConv } = require('#src/constants')
const { tryCatchEndTest } = require('#test/utils')

const createSpanStub = () => Object.freeze({
  setAttribute: sinon.stub(),
  setStatus: sinon.stub(),
  setAttributes: sinon.stub(),
  recordException: sinon.stub(),
  end: sinon.stub()
})

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
    const baseConfig = { // move to DTO or fixtures
      options: { batchSize: 5 },
      rdkafkaConf: {
        'client.id': 'test-client',
        'group.id': 'test-group',
        'metadata.broker.list': 'localhost:9092'
      }
    }

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
    const baseConfig = {
      rdkafkaConf: {
        'client.id': 'producer-client',
        'metadata.broker.list': 'localhost:9092'
      }
    }

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
    const config = {
      rdkafkaConf: {
        'client.id': 'producer-client',
        'metadata.broker.list': 'localhost:9092'
      }
    }

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

  otelSuite.end()
})
