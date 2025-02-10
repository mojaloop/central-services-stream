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
const helpers = require('#src/helpers')
const { tryCatchEndTest } = require('#test/utils')

Test('helpers Tests -->', (helpersTests) => {
  helpersTests.test('should return empty object if kafkaHeaders is not array with elements', tryCatchEndTest((test) => {
    const kafkaHeaders = null
    const otelHeaders = helpers.extractOtelHeaders(kafkaHeaders)
    test.deepEqual(otelHeaders, {})
  }))

  helpersTests.test('should extract OTel headers from array with one-with-all headers element', tryCatchEndTest((test) => {
    const kafkaHeaders = [
      {
        'content-type': 'application/json',
        traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
        tracestate: 'congo=t61rcWkgMzE',
        baggage: 'key1=value1,key2=value2'
      }
    ]
    const otelHeaders = helpers.extractOtelHeaders(kafkaHeaders)
    test.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    test.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    test.ok(otelHeaders.baggage, 'baggage header is extracted')
    test.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  helpersTests.test('should extract OTel headers from array with several headers elements', tryCatchEndTest((test) => {
    const kafkaHeaders = [
      { 'content-type': 'application/json' },
      { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
      { tracestate: 'congo=t61rcWkgMzE' },
      { baggage: 'key1=value1,key2=value2' }
    ]
    const otelHeaders = helpers.extractOtelHeaders(kafkaHeaders)
    test.ok(otelHeaders.traceparent, 'traceparent header is extracted')
    test.ok(otelHeaders.tracestate, 'tracestate header is extracted')
    test.ok(otelHeaders.baggage, 'baggage header is extracted')
    test.false(otelHeaders['content-type'], 'content-type header is NOT extracted')
  }))

  helpersTests.end()
})
