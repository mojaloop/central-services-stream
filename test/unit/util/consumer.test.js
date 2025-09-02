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

 * Georgi Georgiev <georgi.georgiev@modusbox.com>

 --------------
 ******/
'use strict'

const src = '../../../src'
const rewire = require('rewire')
const Sinon = require('sinon')
const Test = require('tapes')(require('tape'))
const Consumer = require(`${src}`).Util.Consumer
const KafkaConsumer = require(`${src}`).Kafka.Consumer
const Logger = require('@mojaloop/central-services-logger')
const { stateList } = require('../../../src/constants')

Test('Consumer', ConsumerTest => {
  let sandbox

  ConsumerTest.beforeEach(test => {
    sandbox = Sinon.createSandbox()
    sandbox.stub(KafkaConsumer.prototype, 'constructor').resolves()
    sandbox.stub(KafkaConsumer.prototype, 'connect').resolves()
    sandbox.stub(KafkaConsumer.prototype, 'consume').resolves()
    sandbox.stub(KafkaConsumer.prototype, 'commitMessageSync').resolves()
    sandbox.stub(Logger, 'isErrorEnabled').value(true)
    sandbox.stub(Logger, 'isWarnEnabled').value(true)
    sandbox.stub(Logger, 'isDebugEnabled').value(true)
    sandbox.stub(Logger, 'isSillyEnabled').value(true)
    test.end()
  })

  ConsumerTest.afterEach(test => {
    sandbox.restore()
    test.end()
  })

  ConsumerTest.test('createHandler should', createHandlerTest => {
    createHandlerTest.test('topic is still added if fails to connect', async (test) => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'admin'
      const config = { rdkafkaConf: {} }
      KafkaConsumer.prototype.connect.throws(new Error())

      // Act
      await ConsumerProxy.createHandler(topicName, config)
      const topics = ConsumerProxy.getListOfTopics()

      // Assert
      test.deepEqual(topics, [topicName], 'Topic should still be in list even if consumer failed to connect.')
      test.end()
    })

    createHandlerTest.test('array topic', async (test) => {
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = ['admin2', 'admin1']
      const config = { rdkafkaConf: {} }
      try {
        await ConsumerProxy.createHandler(topicName, config)
        test.pass('passed')
      } catch (err) {
        test.fail('Error Thrown')
      }
      test.end()
    })

    createHandlerTest.test('should have a timestamp of 0 if couldn\'t connect', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = ['admin2', 'admin1']
      const config = { rdkafkaConf: {} }
      KafkaConsumer.prototype.connect.throws(new Error())

      // Act
      await ConsumerProxy.createHandler(topicName, config)
      const result = ConsumerProxy.__get__('listOfConsumers')
      const timestamps = Object.keys(result).map(k => result[k].connectedTimeStamp)

      // Assert
      test.deepEqual(timestamps, [0, 0], 'Timestamps should be 0')
      test.end()
    })

    createHandlerTest.test('should contain a timestamp of when it connected', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = ['admin2', 'admin1']
      const config = { rdkafkaConf: {} }
      // KafkaConsumer.prototype.connect.throws(new Error())

      // Act
      await ConsumerProxy.createHandler(topicName, config)
      const result = ConsumerProxy.__get__('listOfConsumers')
      const timestamps = Object.keys(result).map(k => result[k].connectedTimeStamp)

      // Assert
      timestamps.forEach(ts => test.ok(ts > 0, 'Timestamp should be greater than 0'))
      test.end()
    })

    createHandlerTest.test('should contain a timestamp of when it connected', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = ['admin2', 'admin1']
      const config = { rdkafkaConf: undefined }
      // KafkaConsumer.prototype.connect.throws(new Error())

      // Act
      await ConsumerProxy.createHandler(topicName, config)
      const result = ConsumerProxy.__get__('listOfConsumers')
      const timestamps = Object.keys(result).map(k => result[k].connectedTimeStamp)

      // Assert
      timestamps.forEach(ts => test.ok(ts > 0, 'Timestamp should be greater than 0'))
      test.end()
    })

    createHandlerTest.test('should set autoCommitEnabled from enableAutoCommit property', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'adminAutoCommit'
      const config = { rdkafkaConf: { enableAutoCommit: false } }
      await ConsumerProxy.createHandler(topicName, config)
      const result = ConsumerProxy.__get__('listOfConsumers')
      test.equal(result[topicName].autoCommitEnabled, false, 'autoCommitEnabled should be set from enableAutoCommit')
      test.end()
    })
    createHandlerTest.end()
  })

  ConsumerTest.test('getListOfTopics should', getListOfTopicsTest => {
    getListOfTopicsTest.test('return an empty array when there are no topics', test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', { })
      const expected = []

      // Act
      const result = ConsumerProxy.getListOfTopics()

      // Assert
      test.deepEqual(result, expected, 'Should return an empty array')
      test.end()
    })

    getListOfTopicsTest.test('return a list of topics', test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', { admin1: {}, admin2: {} })
      const expected = ['admin1', 'admin2']

      // Act
      const result = ConsumerProxy.getListOfTopics()

      // Assert
      test.deepEqual(result, expected, 'Should return an empty array')
      test.end()
    })

    getListOfTopicsTest.end()
  })

  ConsumerTest.test('getConsumer should', getConsumerTest => {
    const topicName = 'admin'
    const expected = 'consumer'

    getConsumerTest.test('return list of consumers', async (test) => {
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {
        admin: {
          consumer: expected
        }
      })
      try {
        const result = await ConsumerProxy.getConsumer(topicName)
        test.equal(result, expected)
      } catch (err) {
        test.fail()
      }
      test.end()
    })

    getConsumerTest.test('throw error', async (test) => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)

      // Act
      try {
        await ConsumerProxy.getConsumer(topicName)
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.pass()
      }
      test.end()
    })

    getConsumerTest.end()
  })

  ConsumerTest.test('isConsumerAutoCommitEnabled should', isConsumerAutoCommitEnabledTest => {
    const topicName = 'admin'

    isConsumerAutoCommitEnabledTest.test('return consumer auto commit status', async (test) => {
      const topicName = 'admin'
      const autoCommitEnabled = false
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', { admin: { autoCommitEnabled } })
      try {
        const result = await ConsumerProxy.isConsumerAutoCommitEnabled(topicName)
        test.equal(result, false, 'auto commit is disabled')
      } catch (err) {
        test.fail()
      }
      test.end()
    })

    isConsumerAutoCommitEnabledTest.test('throw error', async (test) => {
      try {
        await Consumer.isConsumerAutoCommitEnabled(topicName)
        test.fail('Error not thrown!')
      } catch (err) {
        test.pass()
      }
      test.end()
    })

    isConsumerAutoCommitEnabledTest.end()
  })

  ConsumerTest.test('isConnected should', isConnectedTest => {
    isConnectedTest.test('Should return true if consumer.isConnected passes', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {
        admin: {
          consumer: {
            isConnected: () => true
          }
        }
      })
      // Act
      try {
        const response = await ConsumerProxy.isConnected('admin')
        test.equal(response, true, 'Response should be boolean true')
      } catch (err) {
        // Assert
        test.fail('Error not thrown!')
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if consumer.isConnected passes, but topicName not supplied', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {
        admin: {
          consumer: {
            isConnected: () => true
          }
        }
      })
      // Act
      try {
        await ConsumerProxy.isConnected()
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'topicName is undefined.', 'Error message does not match')
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if consumer.isConnected fails', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {
        admin: {
          consumer: {
            // Callback with error
            isConnected: () => {
              throw new Error('test err message.')
            }
          }
        }
      })

      // Act
      try {
        await ConsumerProxy.isConnected('admin')
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'test err message.', 'Error message does not match')
        test.pass()
      }
      test.end()
    })

    isConnectedTest.test('reject with an error if consumer does not exist', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {
        someOtherTopic: {
          consumer: {
            isConnected: () => true
          }
        }
      })

      // Act
      try {
        await ConsumerProxy.isConnected('admin')
        test.fail('Error not thrown!')
      } catch (err) {
        // Assert
        test.equal(err.message, 'No consumer found for topic admin', 'Error message does not match')
        test.pass()
      }
      test.end()
    })

    isConnectedTest.end()
  })

  ConsumerTest.test('getMetadataPromise should', getMetadataPromiseTest => {
    getMetadataPromiseTest.test('resolve with metadata when no error', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: 'admin' }] })
      }
      // Act
      try {
        const metadata = await ConsumerProxy.getMetadataPromise(fakeConsumer, 'admin')
        test.deepEqual(metadata, { topics: [{ name: 'admin' }] }, 'Should resolve with metadata')
      } catch (err) {
        test.fail('Should not throw')
      }
      test.end()
    })

    getMetadataPromiseTest.test('reject with error when getMetadata returns error', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(new Error('fail'), null)
      }
      // Act
      try {
        await ConsumerProxy.getMetadataPromise(fakeConsumer, 'admin')
        test.fail('Should throw')
      } catch (err) {
        test.ok(err.message.includes('Error connecting to consumer: fail'), 'Should reject with error')
      }
      test.end()
    })

    getMetadataPromiseTest.end()
  })

  ConsumerTest.test('allConnected should', allConnectedTest => {
    allConnectedTest.test('return OK if topic found in metadata', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: 'admin' }] }),
        isEventStatsConnectionHealthy: undefined
      }
      ConsumerProxy.__set__('listOfConsumers', {
        admin: { consumer: fakeConsumer }
      })
      // Act
      try {
        const result = await ConsumerProxy.allConnected('admin')
        test.equal(result, stateList.OK, 'Should return OK')
      } catch (err) {
        test.fail('Should not throw')
      }
      test.end()
    })

    allConnectedTest.test('throw error if topic not found in metadata', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: 'otherTopic' }] }),
        isEventStatsConnectionHealthy: undefined
      }
      ConsumerProxy.__set__('listOfConsumers', {
        admin: { consumer: fakeConsumer }
      })
      // Act
      try {
        await ConsumerProxy.allConnected('admin')
        test.fail('Should throw')
      } catch (err) {
        test.ok(err.message.includes('Connected to consumer, but admin not found.'), 'Should throw correct error')
      }
      test.end()
    })

    allConnectedTest.test('throw error if getConsumer throws', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.__set__('listOfConsumers', {})
      // Act
      try {
        await ConsumerProxy.allConnected('admin')
        test.fail('Should throw')
      } catch (err) {
        test.ok(err.message.includes('No consumer found for topic admin'), 'Should throw correct error')
      }
      test.end()
    })

    allConnectedTest.test('throw error if isEventStatsConnectionHealthy returns false', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: 'admin' }] }),
        isEventStatsConnectionHealthy: () => false
      }
      ConsumerProxy.__set__('listOfConsumers', {
        admin: { consumer: fakeConsumer }
      })
      // Act
      try {
        await ConsumerProxy.allConnected('admin')
        test.fail('Should throw')
      } catch (err) {
        test.ok(err.message.includes('Consumer event.stats indicates unhealthy connection for topic admin'), 'Should throw correct error')
      }
      test.end()
    })

    allConnectedTest.test('should throw error if consumerHealth timer marks unhealthy', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'admin'
      // Set consumerHealth to unhealthy
      const consumerHealth = {}
      consumerHealth[topicName] = { healthy: false, timer: {} }
      ConsumerProxy.__set__('consumerHealth', consumerHealth)
      ConsumerProxy.__set__('listOfConsumers', {
        admin: { consumer: { getMetadata: () => {}, isEventStatsConnectionHealthy: undefined } }
      })
      // Act
      try {
        await ConsumerProxy.allConnected(topicName)
        test.fail('Should throw')
      } catch (err) {
        test.ok(
          err.message.includes('Consumer health variable indicates unhealthy connection for topic admin'),
          'Should throw correct error for unhealthy timer'
        )
      }
      test.end()
    })

    allConnectedTest.test('should not throw error if consumerHealth is healthy', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'admin'
      const consumerHealth = {}
      consumerHealth[topicName] = { healthy: true, timer: null }
      ConsumerProxy.__set__('consumerHealth', consumerHealth)
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: topicName }] }),
        isEventStatsConnectionHealthy: undefined
      }
      ConsumerProxy.__set__('listOfConsumers', {
        admin: { consumer: fakeConsumer }
      })
      // Act
      try {
        const result = await ConsumerProxy.allConnected(topicName)
        test.equal(result, stateList.OK, 'Should return OK if healthy')
      } catch (err) {
        test.fail('Should not throw')
      }
      test.end()
    })

    allConnectedTest.test('should throw error if isEventStatsConnectionHealthy exists and returns false', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'admin'
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: topicName }] }),
        isEventStatsConnectionHealthy: () => false
      }
      ConsumerProxy.__set__('listOfConsumers', {
        [topicName]: { consumer: fakeConsumer }
      })
      // Act
      try {
        await ConsumerProxy.allConnected(topicName)
        test.fail('Should throw error when isEventStatsConnectionHealthy returns false')
      } catch (err) {
        test.ok(
          err.message.includes(`Consumer event.stats indicates unhealthy connection for topic ${topicName}`),
          'Should throw correct error for unhealthy event.stats'
        )
      }
      test.end()
    })

    allConnectedTest.test('should not throw error if isEventStatsConnectionHealthy exists and returns true', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'admin'
      const fakeConsumer = {
        getMetadata: (opts, cb) => cb(null, { topics: [{ name: topicName }] }),
        isEventStatsConnectionHealthy: () => true
      }
      ConsumerProxy.__set__('listOfConsumers', {
        [topicName]: { consumer: fakeConsumer }
      })
      // Act
      try {
        const result = await ConsumerProxy.allConnected(topicName)
        test.equal(result, stateList.OK, 'Should return OK if event.stats is healthy')
      } catch (err) {
        test.fail('Should not throw')
      }
      test.end()
    })
    allConnectedTest.end()
  })

  ConsumerTest.test('Health timer functionality', healthTimerTest => {
    healthTimerTest.test('should mark consumer as unhealthy after timer expires on error', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'healthTopic'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      // Fake Consumer with consume method that saves the callback
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(50) // Set short timer

      // Act
      await ConsumerProxy.createHandler(topicName, config, () => {})
      // Simulate error in consume callback
      consumeCallback(new Error('fail'), null)
      // Wait for timer to expire
      setTimeout(() => {
        const consumerHealth = ConsumerProxy.__get__('consumerHealth')
        test.equal(consumerHealth[topicName].healthy, false, 'Should be unhealthy after timer')
        test.end()
      }, 70)
    })

    healthTimerTest.test('should clear health timer and mark healthy on success', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'healthTopic2'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(100)
      await ConsumerProxy.createHandler(topicName, config, () => {})
      // Simulate error to start timer
      consumeCallback(new Error('fail'), null)
      // Simulate success before timer expires
      setTimeout(() => {
        consumeCallback(null, {})
        const consumerHealth = ConsumerProxy.__get__('consumerHealth')
        test.equal(consumerHealth[topicName].healthy, true, 'Should be healthy after success')
        test.equal(consumerHealth[topicName].timer, null, 'Timer should be cleared')
        test.end()
      }, 30)
    })

    healthTimerTest.test('getConsumerHealthTimerMs and setConsumerHealthTimerMs should work', test => {
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      ConsumerProxy.setConsumerHealthTimerMs(1234)
      const ms = ConsumerProxy.getConsumerHealthTimerMs()
      test.equal(ms, 1234, 'Should get the value set')
      test.end()
    })

    healthTimerTest.test('consumeWithHealthTracking should call command with error and set timer', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'testTopic'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      let commandCalled = false
      let commandErrorArg = null
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(50)
      const command = (error, messages) => {
        commandCalled = true
        commandErrorArg = error
      }
      await ConsumerProxy.createHandler(topicName, config, command)
      // Act
      const errorObj = new Error('consume error')
      consumeCallback(errorObj, null)
      // Assert
      setTimeout(() => {
        test.ok(commandCalled, 'command should be called')
        test.equal(commandErrorArg, errorObj, 'command should receive error')
        const consumerHealth = ConsumerProxy.__get__('consumerHealth')
        test.equal(typeof consumerHealth[topicName].timer, 'object', 'timer should be set')
        test.end()
      }, 10)
    })

    healthTimerTest.test('consumeWithHealthTracking should clear timer and mark healthy on success', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'testTopic2'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      let commandCalled = false
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(50)
      const command = (error, messages) => {
        commandCalled = true
        if (error) {
          // Handle or log the error to satisfy standard style
          // eslint-disable-next-line no-console
          console.error(error)
        }
        // Optionally reference messages if needed
        // messages is intentionally unused
      }
      await ConsumerProxy.createHandler(topicName, config, command)
      // Simulate error to start timer
      consumeCallback(new Error('fail'), null)
      // Simulate success to clear timer
      setTimeout(() => {
        consumeCallback(null, { foo: 'bar' })
        const consumerHealth = ConsumerProxy.__get__('consumerHealth')
        test.equal(consumerHealth[topicName].healthy, true, 'Should be healthy after success')
        test.equal(consumerHealth[topicName].timer, null, 'Timer should be cleared')
        test.ok(commandCalled, 'command should be called')
        test.end()
      }, 10)
    })

    healthTimerTest.test('consumeWithHealthTracking should not start multiple timers for repeated errors', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'testTopic3'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(30)
      const command = () => {}
      await ConsumerProxy.createHandler(topicName, config, command)
      // Simulate error to start timer
      consumeCallback(new Error('fail1'), null)
      const consumerHealth = ConsumerProxy.__get__('consumerHealth')
      const timer1 = consumerHealth[topicName].timer
      // Simulate another error before timer expires
      consumeCallback(new Error('fail2'), null)
      const timer2 = consumerHealth[topicName].timer
      test.equal(timer1, timer2, 'Timer should not be replaced on repeated errors')
      setTimeout(() => {
        test.equal(consumerHealth[topicName].healthy, false, 'Should be unhealthy after timer')
        test.end()
      }, 40)
    })

    healthTimerTest.test('consumeWithHealthTracking should initialize consumerHealth entry if missing', async test => {
      // Arrange
      const ConsumerProxy = rewire(`${src}/util/consumer`)
      const topicName = 'testTopic4'
      const config = { rdkafkaConf: {} }
      let consumeCallback
      function FakeConsumer () {}
      FakeConsumer.prototype.connect = async () => {}
      FakeConsumer.prototype.consume = cb => { consumeCallback = cb }
      ConsumerProxy.__set__('Consumer', FakeConsumer)
      ConsumerProxy.setConsumerHealthTimerMs(20)
      const command = () => {}
      await ConsumerProxy.createHandler(topicName, config, command)
      // Remove consumerHealth entry to simulate missing
      const consumerHealth = ConsumerProxy.__get__('consumerHealth')
      delete consumerHealth[topicName]
      // Act
      consumeCallback(new Error('fail'), null)
      // Assert
      setTimeout(() => {
        const consumerHealth2 = ConsumerProxy.__get__('consumerHealth')
        test.ok(consumerHealth2[topicName], 'consumerHealth entry should be initialized')
        test.equal(consumerHealth2[topicName].healthy, false, 'Should be unhealthy after timer')
        test.end()
      }, 30)
    })
    healthTimerTest.end()
  })

  ConsumerTest.end()
})
