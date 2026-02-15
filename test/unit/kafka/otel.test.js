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
const { tryCatchEndTest } = require('#test/utils')

const createSpanStub = () => Object.freeze({
  setStatus: sinon.stub(),
  setAttributes: sinon.stub(),
  recordException: sinon.stub(),
  end: sinon.stub()
})

Test('otel Tests -->', (otelTests) => {
  otelTests.test('should return empty object if kafkaHeaders is not array with elements', tryCatchEndTest((test) => {
    const kafkaHeaders = null
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    test.deepEqual(otelHeaders, {})
  }))

  otelTests.test('should extract OTel headers from array with one-with-all headers element', tryCatchEndTest((test) => {
    const kafkaHeaders = [
      {
        'content-type': 'application/json',
        traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
        tracestate: 'congo=t61rcWkgMzE',
        baggage: 'key1=value1,key2=value2'
      }
    ]
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    test.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    test.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    test.ok(otelHeaders.baggage, 'baggage header is extracted')
    test.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  otelTests.test('should extract OTel headers from array with several headers elements', tryCatchEndTest((test) => {
    const kafkaHeaders = [
      { 'content-type': 'application/json' },
      { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
      { tracestate: 'congo=t61rcWkgMzE' },
      { baggage: 'key1=value1,key2=value2' }
    ]
    const otelHeaders = otel.extractOtelHeaders(kafkaHeaders)
    test.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    test.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    test.ok(otelHeaders.baggage, 'baggage header is extracted')
    test.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  otelTests.test('executeAndSetSpanStatus Tests -->', (executeMethodTests) => {
    executeMethodTests.test('should set error status and record exception in case if fn failed', tryCatchEndTest(async (test) => {
      const error = new Error('Test error')
      const fn = () => { throw error }
      const span = createSpanStub()
      const withSpanEnd = false
      const rethrowError = false

      await otel.executeAndSetSpanStatus(fn, span, withSpanEnd, rethrowError)
      test.true(span.setStatus.calledWith({ code: SpanStatusCode.ERROR }), 'span.setStatus() was called with error code')
      test.true(span.recordException.calledWith(error), 'span.recordException() was called with error')
    }))

    executeMethodTests.test('should call span.end() if withSpanEnd === true', tryCatchEndTest(async (test) => {
      const fn = () => {}
      const span = createSpanStub()
      const withSpanEnd = true

      await otel.executeAndSetSpanStatus(fn, span, withSpanEnd)
      test.true(span.end.calledOnce, 'span.end() was called')
    }))

    executeMethodTests.end()
  })

  otelTests.test('makeConsumerAttributes Tests -->', (attrTests) => {
    const baseConfig = { // move to DTO or fixtures
      options: { batchSize: 5 },
      rdkafkaConf: {
        'client.id': 'test-client',
        'group.id': 'test-group',
        'metadata.broker.list': 'localhost:9092'
      }
    }

    attrTests.test('should use actual count from array payload', tryCatchEndTest((test) => {
      const payload = [{ value: 1 }, { value: 2 }, { value: 3 }]
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      test.equal(attrs['messaging.batch.message_count'], 3, 'count should be 3 for array of 3')
    }))

    attrTests.test('should use count=1 for single message payload', tryCatchEndTest((test) => {
      const payload = { value: 1 }
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic', payload)
      test.equal(attrs['messaging.batch.message_count'], 1, 'count should be 1 for single message')
    }))

    attrTests.test('should fall back to config.options.batchSize when no payload', tryCatchEndTest((test) => {
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic')
      test.equal(attrs['messaging.batch.message_count'], 5, 'count should fall back to batchSize=5')
    }))

    attrTests.test('should set all standard consumer attributes', tryCatchEndTest((test) => {
      const attrs = otel.makeConsumerAttributes(baseConfig, 'test-topic')
      test.equal(attrs['messaging.client.id'], 'test-client')
      test.equal(attrs['messaging.consumer.group.name'], 'test-group')
      test.equal(attrs['messaging.destination.name'], 'test-topic')
      test.equal(attrs['messaging.operation.name'], 'receive')
      test.equal(attrs['messaging.system'], 'kafka')
      test.equal(attrs['server.address'], 'localhost:9092')
    }))

    attrTests.end()
  })

  otelTests.end()
})
