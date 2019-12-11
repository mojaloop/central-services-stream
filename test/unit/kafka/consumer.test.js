/* eslint-disable prefer-promise-reject-errors */
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

 * Lazola Lucas <lazola.lucas@modusbox.com>
 * Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 * Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/

/**
 * Kafka Consumer
 * @module Consumer
 */

'use strict'

const Test = require('tapes')(require('tape'))
const Consumer = require('../../../src/kafka').Consumer
const ConsumerEnums = require('../../../src/kafka').Consumer.ENUMS
const Logger = require('@mojaloop/central-services-logger')
const Kafka = require('node-rdkafka')
const Sinon = require('sinon')
const KafkaStubs = require('./KafkaStub')

Test('Consumer test', (consumerTests) => {
  let sandbox
  // let clock
  let config = {}
  let topicsList = []

  // lets setup the tests
  consumerTests.beforeEach((test) => {
    sandbox = Sinon.createSandbox()
    // clock = Sinon.useFakeTimers({
    //   now: Date.now(),
    //   shouldAdvanceTime: true
    // })

    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    topicsList = ['test']

    sandbox.stub(Kafka, 'KafkaConsumer').callsFake(
      () => {
        const k = new KafkaStubs.KafkaConsumer()
        return k
      }
    )

    test.end()
  })

  // lets tear down the tests
  consumerTests.afterEach((test) => {
    sandbox.restore()
    test.end()
  })

  consumerTests.test('Test Consumer::constructor', (assert) => {
    const ConsumerSpy = Sinon.spy(Consumer.prototype, 'constructor')
    const c = new ConsumerSpy(topicsList, config)
    assert.ok(c, 'Consumer instance created')
    assert.ok(ConsumerSpy.calledOnce, 'Consumer constructor called once')
    ConsumerSpy.restore()
    assert.end()
  })

  consumerTests.test('Test Consumer::constructor - no config', (assert) => {
    try {
      const c = new Consumer(topicsList, {})
      assert.ok(c, 'Consumer instance created')
      assert.end()
    } catch (error) {
      Logger.error(error)
      assert.equals(error.message.toString(), 'missing a config object')
      assert.end()
    }
  })

  consumerTests.test('Test Consumer::constructor - no params', (assert) => {
    try {
      const c = new Consumer()
      assert.ok(c, true)
      assert.end()
    } catch (e) {
      console.log('ERRRRRRRR', e)
      assert.fail()
      assert.end()
    }
  })

  consumerTests.test('Test Consumer::connect', (assert) => {
    assert.plan(2)
    const c = new Consumer(topicsList, config)
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
    })
  })

  consumerTests.test('Test Consumer::connect - with error on callBack', (assert) => {
    sandbox.stub(KafkaStubs.KafkaConsumer.prototype, 'connect').callsFake(
      (_err, info) => {
        info('error test test', null)
      }
    )

    assert.plan(2)
    const c = new Consumer(topicsList, config)

    // consume 'message' event
    c.on('error', error => {
      Logger.error(error)
      assert.ok(Sinon.match(error, 'error test test'), 'on Error event received')
    })

    c.connect().then(() => {
    }).catch((error) => {
      assert.ok(Sinon.match(error, 'Unhandled "error" event. (error test test)'))
    })
  })

  consumerTests.test('Test Consumer::disconnect', (assert) => {
    const discoCallback = (err, metrics) => {
      if (err) {
        Logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    const c = new Consumer(topicsList, config)
    c.connect().then(() => {
      c.disconnect(discoCallback)
    })
  })

  consumerTests.test('Test Consumer::disconnect - no callback', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(() => {
      c.disconnect()
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::subscribe', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(() => {
      c.subscribe(topicsList)
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::subscribe - no params', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(() => {
      c.subscribe()
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::getWatermarkOffsets', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      const waterMarkOffset = c.getWatermarkOffsets(topicsList, 0)
      assert.ok(waterMarkOffset, 'waterMarkOffset result exists')
      assert.deepEqual(waterMarkOffset, KafkaStubs.watermarkOffsetSampleStub, 'waterMarkOffset results match')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::getMetadata', (assert) => {
    const metaDatacCb = (error, metadata) => {
      if (error) {
        Logger.error(error)
      }
      assert.ok(metadata, 'metadata object exists')
      assert.deepEqual(metadata, KafkaStubs.metadataSampleStub, 'metadata objects match')
      assert.end()
    }
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.getMetadata(null, metaDatacCb)
    })
  })

  consumerTests.test('Test Consumer::getMetadata - no callback function', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.getMetadata(null)
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commit', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commit(topicsList)
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commit - no params', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commit()
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commitSync', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commitSync(topicsList)
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commitSync - no params', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commitSync()
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commitMessage', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commitMessage(KafkaStubs.messageSampleStub)
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::commitMessageSync', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.commitMessageSync(KafkaStubs.messageSampleStub)
      assert.pass('commit passed')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::consumeOnce - Not Implemented - default params', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consumeOnce()
      } catch (error) {
        Logger.error(error)
        assert.equals(error.message.toString(), 'Not implemented')
        assert.end()
      }
    })
  })

  consumerTests.test('Test Consumer::consumeOnce - Not Implemented - batchSize=10', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consumeOnce(1, (error) => {
          return new Promise((resolve, reject) => {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            resolve(true)
          })
        })
      } catch (error) {
        Logger.error(error)
        assert.equals(error.message.toString(), 'Not implemented')
        assert.end()
      }
    })
  })

  consumerTests.test('Test Consumer::consume - defaults', (assert) => {
    let messageReceived = false

    const c = new Consumer(topicsList, config)

    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      c.disconnect()
      assert.ok(message, 'on Message event received')
      if (!messageReceived) {
        assert.end()
        messageReceived = true
      }
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.consume()
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=false, messageAsJson=true', (assert) => {
    assert.plan(5)
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            Logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message.length != null && message.length > 0) {
              message.forEach(msg => {
                c.commitMessage(msg)
              })
            } else {
              c.commitMessage(message)
            }
            resolve(true)
            assert.equals(typeof message.value, 'object')
            assert.ok(message, 'message processed')
          } else {
            resolve(false)
            assert.fail('message not processed')
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=false, messageAsJson=false', (assert) => {
    assert.plan(5)
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    c.on('error', error => {
      Logger.error(`error: ${error}`)
    })

    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            Logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message.length != null && message.length > 0) {
              message.forEach(msg => {
                c.commitMessage(msg)
              })
            } else {
              c.commitMessage(message)
            }
            resolve(true)
            assert.equals(typeof message.value, 'string')
            assert.ok(message, 'message processed')
          } else {
            resolve(false)
            assert.fail('message not processed')
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=true, messageAsJson=true', (assert) => {
    assert.plan(5)
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            Logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message.length != null && message.length > 0) {
              message.forEach(msg => {
                c.commitMessage(msg)
              })
            } else {
              c.commitMessage(message)
            }
            resolve(true)
            assert.ok(message, 'message processed')
            assert.equals(typeof message.value, 'object')
          } else {
            resolve(false)
            assert.fail('message not processed')
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=true, messageAsJson=false', (assert) => {
    assert.plan(5)
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            Logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message.length != null && message.length > 0) {
              message.forEach(msg => {
                c.commitMessage(msg)
              })
            } else {
              c.commitMessage(message)
            }
            resolve(true)
            assert.ok(message, 'message processed')
            assert.equals(typeof message.value, 'string')
          } else {
            resolve(false)
            assert.fail('message not processed')
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=true, messageAsJson=false with callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    let consumeCount = 0
    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`OMG - ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
        })
      })

      c.consume((error, message) => {
        return new Promise(() => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          throw new Error(errorMessageThrown)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          reject(errorMessageRejected)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
          processedNextMessage = true
          if (errorHandledThrown && errorHandledRejected) {
            assert.pass('All errors handled')
            assert.end()
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=false, messageAsJson=false with callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.flow,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    let consumeCount = 0
    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`onError: ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
        })
      })

      c.consume((error, message) => {
        return new Promise(() => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          throw new Error(errorMessageThrown)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          reject(errorMessageRejected)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          Logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
          processedNextMessage = true
          if (errorHandledThrown && errorHandledRejected) {
            assert.pass('All errors handled')
            assert.end()
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=false, messageAsJson=true', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let pollCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          if (pollCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the poller consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'object')
              })
            } else {
              resolve(false)
              c.disconnect()
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=false, messageAsJson=false', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let pollCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          if (pollCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the poller consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'string')
              })
            } else {
              resolve(false)
              c.disconnect()
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=true, messageAsJson=true', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let pollCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          if (pollCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the poller consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'object')
              })
            } else {
              resolve(false)
              c.disconnect()
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=true, messageAsJson=false', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let pollCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          if (pollCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the poller consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'string')
              })
            } else {
              resolve(false)
              c.disconnect()
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=false, messageAsJson=true', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let recursiveCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          if (recursiveCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the recursive consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'object')
              })
            } else {
              resolve(false)
              assert.fail('message not processed')
              assert.end()
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=false, messageAsJson=false', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let recursiveCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          if (recursiveCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the recursive consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'string')
              })
            } else {
              resolve(false)
              assert.fail('message not processed')
              assert.end()
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=true, messageAsJson=true', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let recursiveCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          if (recursiveCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the recursive consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'object')
              })
            } else {
              resolve(false)
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=true, messageAsJson=false', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    let recursiveCount = 0

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          if (recursiveCount > 1) {
            c.disconnect()
            assert.ok(true, 'Message processed once by the recursive consumer')
            assert.end()
          } else {
            if (error) {
              Logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
              // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
              if (Array.isArray(message) && message.length != null && message.length > 0) {
                message.forEach(msg => {
                  c.commitMessage(msg)
                })
              } else {
                c.commitMessage(message)
              }
              resolve(true)
              assert.ok(message, 'message processed')
              assert.ok(Array.isArray(message), 'batch of messages received')
              message.forEach(msg => {
                assert.equals(typeof msg.value, 'string')
              })
            } else {
              resolve(false)
              assert.fail('message not processed')
            }
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=true, messageAsJson=false with callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    let recursiveCount = 0
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`OMG - ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          Logger.info(`consume::callback[recursiveCount=${recursiveCount}] ${error}, ${JSON.stringify(message)}`)
          if (recursiveCount > 3) {
            c.disconnect()
            assert.ok(true, 'Message processed by the recursive consumer')
            processedNextMessage = true
            if (errorHandledThrown && errorHandledRejected) {
              assert.pass('All errors handled')
              assert.end()
            }
          } else if (recursiveCount === 2) {
            throw new Error(errorMessageThrown)
          } else if (recursiveCount === 3) {
            reject(errorMessageRejected)
          } else {
            resolve(true)
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=false, messageAsJson=false with callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: false,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    let recursiveCount = 0
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`OMG - ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    c.on('batch', messages => {
      Logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          Logger.info(`consume::callback[recursiveCount=${recursiveCount}] ${error}, ${JSON.stringify(message)}`)
          if (recursiveCount > 3) {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
            c.disconnect()
            processedNextMessage = true
            if (errorHandledThrown && errorHandledRejected) {
              assert.pass('All errors handled')
              assert.end()
            }
          } else if (recursiveCount === 2) {
            throw new Error(errorMessageThrown)
          } else if (recursiveCount === 3) {
            reject(errorMessageRejected)
          } else {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=false, messageAsJson=true, batchSize=0', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 0,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consume()
      } catch (error) {
        Logger.error(error)
        c.disconnect()
        assert.equals(error.message.toString(), 'batchSize option is not valid - Select an integer greater then 0')
        assert.end()
      }
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=false, messageAsJson=true, batchSize=0 with Callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 10,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let pollCount = 0
    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`OMG - ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          Logger.info(`consume::callback[recursiveCount=${pollCount}] ${error}, ${JSON.stringify(message)}`)
          if (pollCount > 3) {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
            c.disconnect()
            processedNextMessage = true
            if (errorHandledThrown && errorHandledRejected) {
              assert.pass('All errors handled')
              assert.end()
            }
          } else if (pollCount === 2) {
            throw new Error(errorMessageThrown)
          } else if (pollCount === 3) {
            reject(errorMessageRejected)
          } else {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume poller sync=true, messageAsJson=true, batchSize=0 with Callback exception', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.poll,
        batchSize: 10,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let pollCount = 0
    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      Logger.debug(`OMG - ${error}`)
      if (error instanceof Error) {
        assert.equal(error.message, errorMessageThrown)
        assert.ok(true, 'Error handled by throw')
        errorHandledThrown = true
      } else if (typeof error === 'string') {
        assert.equal(error, errorMessageRejected)
        assert.ok(true, 'Error handled by rejection')
        errorHandledRejected = true
        if (processedNextMessage) {
          assert.pass('All errors handled')
          assert.end()
        }
      }
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          pollCount = pollCount + 1
          Logger.info(`consume::callback[recursiveCount=${pollCount}] ${error}, ${JSON.stringify(message)}`)
          if (pollCount > 3) {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
            c.disconnect()
            processedNextMessage = true
            if (errorHandledThrown && errorHandledRejected) {
              assert.pass('All errors handled')
              assert.end()
            }
          } else if (pollCount === 2) {
            throw new Error(errorMessageThrown)
          } else if (pollCount === 3) {
            reject(errorMessageRejected)
          } else {
            assert.ok(true, 'Message processed by the recursive consumer')
            resolve(true)
          }
        })
      })
    })
  })

  consumerTests.test('Test Consumer::consume recursive sync=false, messageAsJson=true, batchSize=0', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 0,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consume()
      } catch (error) {
        Logger.error(error)
        c.disconnect()
        assert.equals(error.message.toString(), 'batchSize option is not valid - Select an integer greater then 0')
        assert.end()
      }
    })
  })

  consumerTests.test('Test Consumer::consume flow sync=false, messageAsJson=true - invalid CONSUMER MODE SELECTED', (assert) => {
    assert.plan(5)
    config = {
      options: {
        mode: 99, // invalid consumer mode
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      Logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            Logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            Logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message.length != null && message.length > 0) {
              message.forEach(msg => {
                c.commitMessage(msg)
              })
            } else {
              c.commitMessage(message)
            }
            resolve(true)
            assert.equals(typeof message.value, 'object')
            assert.ok(message, 'message processed')
          } else {
            resolve(false)
            assert.fail('message not processed')
          }
        })
      })
    })
  })

  consumerTests.end()
})

Test('Consumer test for KafkaConsumer events', (consumerTests) => {
  let sandbox
  // let clock
  let config = {}
  let topicsList = []

  // lets setup the tests
  consumerTests.beforeEach((test) => {
    sandbox = Sinon.createSandbox()

    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger: Logger
    }

    topicsList = ['test']

    sandbox.stub(Kafka, 'KafkaConsumer').callsFake(
      () => {
        const k = new KafkaStubs.KafkaConsumerForEventTests()
        return k
      }
    )

    test.end()
  })

  // lets tear down the tests
  consumerTests.afterEach((test) => {
    sandbox.restore()
    test.end()
  })

  consumerTests.test('Test Consumer::connect - test KafkaConsumer events: event.log, event.error, error', (assert) => {
    assert.plan(4)
    const c = new Consumer(topicsList, config)

    // consume 'message' event
    c.on('error', error => {
      Logger.error(error)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    c.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
    })
  })

  consumerTests.end()
})
