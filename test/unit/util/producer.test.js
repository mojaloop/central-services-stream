/*****
 License
 --------------
 Copyright © 2020-2025 Mojaloop Foundation
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
 - Name Surname <name.surname@mojaloop.io>

 * Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 * Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/
'use strict'

const src = '../../../src'
const Sinon = require('sinon')
const rewire = require('rewire')
const Test = require('tapes')(require('tape'))
const KafkaProducer = require(`${src}/kafka`).Producer
const Producer = require(`${src}/util`).Producer
const Uuid = require('uuid4')
const Logger = require('@mojaloop/central-services-logger')
const { stateList } = require(`${src}/constants`)

const transfer = {
  transferId: 'b51ec534-ee48-4575-b6a9-ead2955b8999',
  payerFsp: 'dfsp1',
  payeeFsp: 'dfsp2',
  amount: {
    currency: 'USD',
    amount: '433.88'
  },
  ilpPacket: 'AYIBgQAAAAAAAASwNGxldmVsb25lLmRmc3AxLm1lci45T2RTOF81MDdqUUZERmZlakgyOVc4bXFmNEpLMHlGTFGCAUBQU0svMS4wCk5vbmNlOiB1SXlweUYzY3pYSXBFdzVVc05TYWh3CkVuY3J5cHRpb246IG5vbmUKUGF5bWVudC1JZDogMTMyMzZhM2ItOGZhOC00MTYzLTg0NDctNGMzZWQzZGE5OGE3CgpDb250ZW50LUxlbmd0aDogMTM1CkNvbnRlbnQtVHlwZTogYXBwbGljYXRpb24vanNvbgpTZW5kZXItSWRlbnRpZmllcjogOTI4MDYzOTEKCiJ7XCJmZWVcIjowLFwidHJhbnNmZXJDb2RlXCI6XCJpbnZvaWNlXCIsXCJkZWJpdE5hbWVcIjpcImFsaWNlIGNvb3BlclwiLFwiY3JlZGl0TmFtZVwiOlwibWVyIGNoYW50XCIsXCJkZWJpdElkZW50aWZpZXJcIjpcIjkyODA2MzkxXCJ9IgA',
  condition: 'YlK5TZyhflbXaDRPtR5zhCu8FrbgvrQwwmzuH0iQ0AI',
  expiration: '2016-05-24T08:38:08.699-04:00',
  extensionList: {
    extension: [
      {
        key: 'key1',
        value: 'value1'
      },
      {
        key: 'key2',
        value: 'value2'
      }
    ]
  }
}

const messageProtocol = {
  id: transfer.transferId,
  from: transfer.payerFsp,
  to: transfer.payeeFsp,
  type: 'application/json',
  content: {
    header: '',
    payload: transfer
  },
  metadata: {
    event: {
      id: Uuid(),
      type: 'prepare',
      action: 'prepare',
      createdAt: new Date(),
      state: {
        status: 'success',
        code: 0
      }
    }
  },
  pp: ''
}

const topicConf = {
  topicName: 'topic-dfsp1-transfer-prepare',
  key: 'producerTest',
  partition: 0,
  opaqueKey: 0
}

const getProducerWithoutThrowError = (topicName) => {
  try {
    return Producer.getProducer(topicName)
  } catch (err) {
    Logger.warn(`getProducer error: ${err?.message}`)
    return null
  }
}

Test('Producer', producerTest => {
  let sandbox
  const config = {}

  producerTest.test('produceMessage should', produceMessageTest => {
    produceMessageTest.beforeEach(async t => {
      sandbox = Sinon.createSandbox()
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      sandbox.stub(KafkaProducer.prototype, 'constructor').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'connect').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'sendMessage').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'disconnect').returns(Promise.resolve())
      t.end()
    })

    produceMessageTest.afterEach(async t => {
      sandbox.restore()
      t.end()
    })

    produceMessageTest.test('return true', async test => {
      const result = await Producer.produceMessage(messageProtocol, topicConf, config)
      test.equal(result, true)
      await Producer.disconnect(topicConf.topicName)
      test.end()
    })

    produceMessageTest.test('disconnect specific topic correctly', async test => {
      try {
        topicConf.topicName = 'someTopic'
        await Producer.produceMessage(messageProtocol, topicConf, config)
        await Producer.disconnect(topicConf.topicName)
        test.pass('Disconnect specific topic successfully')
        test.end()
      } catch (e) {
        test.fail('Error thrown')
        test.end()
      }
    })

    produceMessageTest.test('disconnect all topics correctly', async test => {
      try {
        topicConf.topicName = 'someTopic1'
        await Producer.produceMessage(messageProtocol, topicConf, config)
        topicConf.topicName = 'someTopic2'
        await Producer.produceMessage(messageProtocol, topicConf, config)
        await Producer.disconnect()
        test.pass('Disconnected all topics successfully')
        test.end()
      } catch (e) {
        test.fail('Error thrown')
        test.end()
      }
    })

    produceMessageTest.end()
  })

  producerTest.test('getProducer should', getProducerTest => {
    getProducerTest.beforeEach(t => {
      sandbox = Sinon.createSandbox()
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      sandbox.stub(KafkaProducer.prototype, 'constructor').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'connect').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'sendMessage').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'disconnect').returns(Promise.resolve())
      t.end()
    })

    getProducerTest.afterEach(t => {
      sandbox.restore()
      t.end()
    })

    getProducerTest.test('fetch a specific Producers', async test => {
      await Producer.produceMessage({}, { topicName: 'test' }, {})
      test.ok(Producer.getProducer('test'))
      test.end()
    })

    getProducerTest.test('throw an exception for a specific Producers not found', async test => {
      try {
        test.ok(Producer.getProducer('undefined'))
        test.fail('Error not thrown!')
      } catch (e) {
        test.ok(e.message === 'No producer found for topic undefined')
      }
      test.end()
    })

    getProducerTest.end()
  })

  producerTest.test('disconnect should', disconnectTest => {
    disconnectTest.beforeEach(t => {
      sandbox = Sinon.createSandbox()
      sandbox.stub(KafkaProducer.prototype, 'constructor').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'connect').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'sendMessage').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'disconnect').returns(Promise.resolve())
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      t.end()
    })

    disconnectTest.afterEach(t => {
      sandbox.restore()
      t.end()
    })

    disconnectTest.test('disconnect from kafka', async test => {
      await Producer.produceMessage({}, { topicName: 'test' }, {})
      test.ok(Producer.disconnect('test'))
      test.end()
    })

    disconnectTest.test('disconnect specific topic correctly', async test => {
      try {
        const topicName = 'someTopic'
        test.ok(await Producer.produceMessage({}, { topicName }, {}))
        await Producer.disconnect(topicName)
        test.pass('Disconnect specific topic successfully')
        const producer = getProducerWithoutThrowError(topicName)
        test.equal(producer, null, 'No disconnected producer')
        await Producer.produceMessage({}, { topicName }, {})
        test.pass('created a new producer for the same topic')
        test.ok(Producer.getProducer(topicName))
        test.end()
      } catch (e) {
        test.fail(`Error thrown: ${e.message}`)
        test.end()
      }
    })

    disconnectTest.test('disconnect all topics correctly', async test => {
      try {
        let topicName = 'someTopic1'
        test.ok(await Producer.produceMessage({}, { topicName }, {}))
        await Producer.disconnect(topicName)
        topicName = 'someTopic2'
        test.ok(await Producer.produceMessage({}, { topicName }, {}))
        await Producer.disconnect()
        test.pass('Disconnected all topics successfully')
        const producer = getProducerWithoutThrowError(topicName)
        test.equal(producer, null, 'No disconnected producer')
        test.end()
      } catch (e) {
        test.fail(`Error thrown: ${e.message}`)
        test.end()
      }
    })

    disconnectTest.test('throw error if failure to disconnect from kafka when disconnecting all Producers', async test => {
      let getProducerStub
      const topicNameSuccess = 'topic1'
      const topicNameFailure = 'topic2'
      try {
        // setup stubs for getProducer method
        getProducerStub = sandbox.stub()
        getProducerStub.returns(new KafkaProducer({}))
        getProducerStub.withArgs(topicNameFailure).throws(`No producer found for topic ${topicNameFailure}`)

        // lets rewire the producer import
        const KafkaProducerProxy = rewire(`${src}/util/producer`)

        // lets override the getProducer method within the import
        KafkaProducerProxy.__set__('getProducer', getProducerStub)

        await KafkaProducerProxy.produceMessage({}, { topicName: topicNameSuccess }, {})
        await KafkaProducerProxy.produceMessage({}, { topicName: topicNameFailure }, {})

        await KafkaProducerProxy.disconnect()

        test.fail()
        test.end()
      } catch (e) {
        test.ok(e instanceof Error)
        test.end()
      }
      getProducerStub.restore()
    })

    disconnectTest.test('throw error if failure to disconnect from kafka if topic does not exist', async test => {
      try {
        const topicName = 'someTopic'
        await Producer.produceMessage({}, { topicName }, {})
        await Producer.disconnect('undefined')
      } catch (e) {
        test.ok(e instanceof Error)
        test.end()
      }
    })

    disconnectTest.test('throw error when a non-string value is passed into disconnect', async (test) => {
      try {
        const badTopicName = {}
        await Producer.disconnect(badTopicName)
        test.fail('Error not thrown')
        test.end()
      } catch (e) {
        test.pass('Error Thrown')
        test.end()
      }
    })

    disconnectTest.end()
  })

  producerTest.test('produceMessage failure should', produceMessageTest => {
    produceMessageTest.beforeEach(t => {
      sandbox = Sinon.createSandbox()
      sandbox.stub(KafkaProducer.prototype, 'constructor').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'connect').throws(new Error())
      sandbox.stub(KafkaProducer.prototype, 'sendMessage').returns(Promise.resolve())
      sandbox.stub(KafkaProducer.prototype, 'disconnect').throws(new Error())
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      t.end()
    })

    produceMessageTest.afterEach(t => {
      sandbox.restore()
      t.end()
    })

    produceMessageTest.test('throw error when connect throws error', async test => {
      try {
        topicConf.topicName = 'invalidTopic'
        await Producer.produceMessage(messageProtocol, topicConf, config)
        test.fail('Error not thrown')
        test.end()
      } catch (e) {
        test.pass('Error thrown')
        test.end()
      }
    })

    produceMessageTest.end()
  })

  producerTest.test('isConnected should', isConnectedTest => {
    isConnectedTest.test('Should return true if producer.isConnected passes', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          // Callback with error
          isConnected: () => true
        }
      })
      // Act
      try {
        const response = await ProducerProxy.isConnected('admin')
        test.equal(response, true, 'Response should be boolean true')
      } catch (err) {
        // Assert
        test.fail('Error not thrown!')
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if producer.isConnected passes, but topicName not supplied', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          // Callback with error
          isConnected: () => true
        }
      })
      // Act
      try {
        await ProducerProxy.isConnected()
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'topicName is undefined.', 'Error message does not match')
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if producer.isConnected fails', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          // Callback with error
          isConnected: () => {
            throw new Error('test err message.')
          }
        }
      })

      // Act
      try {
        await ProducerProxy.isConnected('admin')
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'test err message.', 'Error message does not match')
        test.pass()
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if producer does not exist', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        someOtherTopic: {
          // Callback with error
          isConnected: () => true
        }
      })

      // Act
      try {
        await ProducerProxy.isConnected('admin')
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'No producer found for topic admin', 'Error message does not match')
        test.pass()
      }
      test.end()
    })

    isConnectedTest.test('allConnected should return OK if all producers are healthy', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      const metadata = {
        orig_broker_id: 0,
        orig_broker_name: 'kafka:9092/0',
        topics: [
          { name: 'admin', partitions: [] }
        ],
        brokers: [{ id: 0, host: 'kafka', port: 9092 }]
      }
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(null, metadata)
        }
      })

      // Act
      let result
      try {
        result = await ProducerProxy.allConnected()
      } catch (err) {
        test.fail(err.message)
      }

      // Assert
      test.equal(result, stateList.OK, 'allConnected should return OK')
      test.end()
    })

    isConnectedTest.test('allConnected should throw if a topic is missing in metadata', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      const metadata = {
        orig_broker_id: 0,
        orig_broker_name: 'kafka:9092/0',
        topics: [
          { name: 'not-admin', partitions: [] }
        ],
        brokers: [{ id: 0, host: 'kafka', port: 9092 }]
      }
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(null, metadata)
        }
      })

      // Act
      try {
        await ProducerProxy.allConnected()
        test.fail('Error not thrown')
      } catch (err) {
      // Assert
        test.ok(err.message.includes('not found in metadata'), 'Should throw error for missing topic')
      }
      test.end()
    })

    isConnectedTest.test('allConnected should throw if isEventStatsConnectionHealthy returns false', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          isEventStatsConnectionHealthy: () => false
        }
      })

      // Act
      try {
        await ProducerProxy.allConnected()
        test.fail('Error not thrown')
      } catch (err) {
      // Assert
        test.ok(err.message.includes('is not healthy'), 'Should throw error for unhealthy connection')
      }
      test.end()
    })

    isConnectedTest.test('allConnected should throw if getMetadata returns error', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(new Error('metadata error'))
        }
      })

      // Act
      try {
        await ProducerProxy.allConnected()
        test.fail('Error not thrown')
      } catch (err) {
      // Assert
        test.ok(err.message.includes('Error connecting to producer'), 'Should throw error for getMetadata error')
      }
      test.end()
    })

    isConnectedTest.test('allConnected should throw if producerHealth is unhealthy for a topic', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      const metadata = {
        orig_broker_id: 0,
        orig_broker_name: 'kafka:9092/0',
        topics: [
          { name: 'admin', partitions: [] }
        ],
        brokers: [{ id: 0, host: 'kafka', port: 9092 }]
      }
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(null, metadata)
        }
      })
      // Set producerHealth to unhealthy
      ProducerProxy.__set__('producerHealth', {
        admin: { healthy: false, timer: null }
      })

      // Act
      try {
        await ProducerProxy.allConnected()
        test.fail('Error not thrown')
      } catch (err) {
        // Assert
        test.ok(err.message.includes('is not healthy'), 'Should throw error for unhealthy producerHealth')
      }
      test.end()
    })

    isConnectedTest.test('allConnected should pass if producerHealth is healthy for a topic', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      const metadata = {
        orig_broker_id: 0,
        orig_broker_name: 'kafka:9092/0',
        topics: [
          { name: 'admin', partitions: [] }
        ],
        brokers: [{ id: 0, host: 'kafka', port: 9092 }]
      }
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(null, metadata)
        }
      })
      // Set producerHealth to healthy
      ProducerProxy.__set__('producerHealth', {
        admin: { healthy: true, timer: null }
      })

      // Act
      let result
      try {
        result = await ProducerProxy.allConnected()
      } catch (err) {
        test.fail(err.message)
      }
      // Assert
      test.equal(result, stateList.OK, 'allConnected should return OK when producerHealth is healthy')
      test.end()
    })

    isConnectedTest.test('allConnected should fallback to metadata check if producerHealth is not set', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      const metadata = {
        orig_broker_id: 0,
        orig_broker_name: 'kafka:9092/0',
        topics: [
          { name: 'admin', partitions: [] }
        ],
        brokers: [{ id: 0, host: 'kafka', port: 9092 }]
      }
      ProducerProxy.__set__('listOfProducers', {
        admin: {
          getMetadata: (options, cb) => cb(null, metadata)
        }
      })
      // producerHealth is not set at all
      ProducerProxy.__set__('producerHealth', {})

      // Act
      let result
      try {
        result = await ProducerProxy.allConnected()
      } catch (err) {
        test.fail(err.message)
      }
      // Assert
      test.equal(result, stateList.OK, 'allConnected should return OK when producerHealth is not set')
      test.end()
    })
    isConnectedTest.end()
  })

  producerTest.test('connectAll should', async connectAllTest => {
    connectAllTest.beforeEach(t => {
      sandbox = Sinon.createSandbox()
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      t.end()
    })

    connectAllTest.afterEach(t => {
      sandbox.restore()
      t.end()
    })

    await connectAllTest.test('register all producers but not ones that are already registered', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__('listOfProducers', {
        test2: {
          producer: {}
        }
      })

      // Act
      const topicConfig = {
        topicName: 'test2',
        key: 'producerTest',
        partition: 0,
        opaqueKey: 0
      }
      const configs = [{
        topicConfig,
        kafkaConfig: config
      }]
      try {
        await ProducerProxy.connectAll(configs)
      } catch (err) {
        test.fail(err.message)
        test.end()
      }

      // Assert
      test.pass()
      test.end()
    })

    await connectAllTest.test('register all producers for the supplied config', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      KafkaProducer.prototype.connect = () => { return Promise.resolve(true) }
      ProducerProxy.__set__({ Producer: KafkaProducer })
      ProducerProxy.__set__('listOfProducers', {
        test1: {
          producer: {}
        }
      })

      // Act
      const topicConfig = {
        topicName: 'admin2',
        key: 'producerTest',
        partition: 0,
        opaqueKey: 0
      }
      const configs = [{
        topicConfig,
        kafkaConfig: config
      }]
      try {
        await ProducerProxy.connectAll(configs)
      } catch (err) {
        test.fail(err.message)
        test.end()
      }

      // Assert
      test.pass()
      test.end()
    })

    await connectAllTest.test('Log errors but not cancel the process', async test => {
      // Arrange
      const ProducerProxy = rewire(`${src}/util/producer`)
      ProducerProxy.__set__({ Producer: () => { throw new Error() } })
      ProducerProxy.__set__('listOfProducers', {
        test3: {
          producer: {}
        }
      })

      // Act
      const topicConfig = {
        topicName: 'admin3',
        key: 'producerTest',
        partition: 0,
        opaqueKey: 0
      }
      const configs = [{
        topicConfig,
        kafkaConfig: config
      }]
      try {
        await ProducerProxy.connectAll(configs)
        test.fail()
        test.end()
      } catch (err) {
        test.pass(err.message)
        test.end()
      }
    })

    connectAllTest.end()
  })

  producerTest.test('producer health timer functionality', healthTest => {
    let ProducerProxy
    let clock

    healthTest.beforeEach(t => {
      sandbox = Sinon.createSandbox()
      clock = sandbox.useFakeTimers()
      ProducerProxy = rewire(`${src}/util/producer`)
      sandbox.stub(Logger, 'isErrorEnabled').value(true)
      sandbox.stub(Logger, 'isDebugEnabled').value(true)
      t.end()
    })

    healthTest.afterEach(t => {
      sandbox.restore()
      t.end()
    })

    healthTest.test('should get and set producer health timer ms', async test => {
      const defaultMs = ProducerProxy.getProducerHealthTimerMs()
      test.ok(typeof defaultMs === 'number', 'Default timer ms is a number')
      ProducerProxy.setProducerHealthTimerMs(5000)
      test.equal(ProducerProxy.getProducerHealthTimerMs(), 5000, 'Timer ms updated')
      ProducerProxy.setProducerHealthTimerMs(defaultMs)
      test.end()
    })

    healthTest.test('should mark producer as unhealthy after timer expires', async test => {
      // Arrange
      const topicName = 'healthTopic'
      const listOfProducers = {}
      const fakeProducer = {
        disconnect: sandbox.stub().resolves()
      }
      listOfProducers[topicName] = fakeProducer
      ProducerProxy.__set__('listOfProducers', listOfProducers)
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Act: update health to false, should start timer
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      test.equal(producerHealth[topicName].healthy, false, 'Producer marked unhealthy')
      test.ok(producerHealth[topicName].timer, 'Timer is set')

      // Fast-forward time
      clock.tick(1001)
      // Timer callback should have run
      test.equal(producerHealth[topicName].healthy, false, 'Producer remains unhealthy after timer')

      // Clean up
      await ProducerProxy.disconnect(topicName)
      test.end()
    })

    healthTest.test('should clear timer and mark healthy if updateProducerHealth called with true', async test => {
      // Arrange
      const topicName = 'healthTopic2'
      const listOfProducers = {}
      const fakeProducer = {
        disconnect: sandbox.stub().resolves()
      }
      listOfProducers[topicName] = fakeProducer
      ProducerProxy.__set__('listOfProducers', listOfProducers)
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Set unhealthy first
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      test.equal(producerHealth[topicName].healthy, false, 'Producer marked unhealthy')
      test.ok(producerHealth[topicName].timer, 'Timer is set')

      // Now set healthy, should clear timer
      ProducerProxy.__get__('updateProducerHealth')(topicName, true)
      test.equal(producerHealth[topicName].healthy, true, 'Producer marked healthy')
      test.equal(producerHealth[topicName].timer, null, 'Timer cleared')

      // Fast-forward time to ensure timer does not fire
      clock.tick(1001)
      test.equal(producerHealth[topicName].healthy, true, 'Producer remains healthy after timer')

      // Clean up
      await ProducerProxy.disconnect(topicName)
      test.end()
    })

    healthTest.test('should remove health entry and clear timer on disconnect', async test => {
      // Arrange
      const topicName = 'healthTopic3'
      const listOfProducers = {}
      const fakeProducer = {
        disconnect: sandbox.stub().resolves()
      }
      listOfProducers[topicName] = fakeProducer
      ProducerProxy.__set__('listOfProducers', listOfProducers)
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Set unhealthy to start timer
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      test.ok(producerHealth[topicName], 'Health entry exists')
      test.ok(producerHealth[topicName].timer, 'Timer is set')

      // Disconnect should clear timer and remove health entry
      await ProducerProxy.disconnect(topicName)
      test.notOk(producerHealth[topicName], 'Health entry removed after disconnect')
      test.end()
    })

    healthTest.test('should initialize health entry if not present', async test => {
      // Arrange
      const topicName = 'newHealthTopic'
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Act
      ProducerProxy.__get__('updateProducerHealth')(topicName, true)

      // Assert
      test.ok(producerHealth[topicName], 'Health entry created')
      test.equal(producerHealth[topicName].healthy, true, 'Producer marked healthy')
      test.equal(producerHealth[topicName].timer, null, 'Timer is null')
      test.end()
    })

    healthTest.test('should clear previous timer when updating to unhealthy again', async test => {
      // Arrange
      const topicName = 'timerClearTopic'
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Set unhealthy to start timer
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      const firstTimer = producerHealth[topicName].timer
      test.ok(firstTimer, 'First timer set')

      // Set unhealthy again, should clear previous timer and set new one
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      const secondTimer = producerHealth[topicName].timer
      test.ok(secondTimer, 'Second timer set')
      test.notEqual(firstTimer, secondTimer, 'Timer was replaced')
      test.end()
    })

    healthTest.test('should not throw if updateProducerHealth called on existing healthy entry', async test => {
      // Arrange
      const topicName = 'alreadyHealthyTopic'
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(1000)

      // Set healthy
      ProducerProxy.__get__('updateProducerHealth')(topicName, true)
      test.equal(producerHealth[topicName].healthy, true, 'Producer marked healthy')

      // Call again with healthy
      ProducerProxy.__get__('updateProducerHealth')(topicName, true)
      test.equal(producerHealth[topicName].healthy, true, 'Producer remains healthy')
      test.equal(producerHealth[topicName].timer, null, 'Timer remains null')
      test.end()
    })

    healthTest.test('should set healthy to false after timer expires when unhealthy', async test => {
      // Arrange
      const topicName = 'timerExpireTopic'
      const producerHealth = {}
      ProducerProxy.__set__('producerHealth', producerHealth)
      ProducerProxy.setProducerHealthTimerMs(500)

      // Set unhealthy to start timer
      ProducerProxy.__get__('updateProducerHealth')(topicName, false)
      test.equal(producerHealth[topicName].healthy, false, 'Producer marked unhealthy')
      test.ok(producerHealth[topicName].timer, 'Timer is set')

      // Fast-forward time
      clock.tick(501)
      test.equal(producerHealth[topicName].healthy, false, 'Producer remains unhealthy after timer')
      test.end()
    })
    healthTest.end()
  })

  producerTest.end()
})
