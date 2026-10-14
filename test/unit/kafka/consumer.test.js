/* eslint-disable prefer-promise-reject-errors */
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
 - Name Surname <name.surname@mojaloop.io>
 - Shashikant Hirugade <shashi.mojaloop@gmail.com>

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
const Kafka = require('node-rdkafka')
const Sinon = require('sinon')
const Metrics = require('@mojaloop/central-services-metrics')
const logger = require('../../../src/lib/logger').logger
const Consumer = require('../../../src/kafka').Consumer
const ConsumerEnums = require('../../../src/kafka').Consumer.ENUMS
const KafkaStubs = require('./KafkaStub')
const { tryCatchEndTest } = require('#test/utils')

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

    sandbox.stub(logger, 'isErrorEnabled').value(true)
    sandbox.stub(logger, 'isWarnEnabled').value(true)
    sandbox.stub(logger, 'isDebugEnabled').value(true)
    sandbox.stub(logger, 'isSillyEnabled').value(true)

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
        clientId: 'default-client',
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger
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
      logger.error(error)
      assert.equals(error.message.toString(), 'missing a config object')
      assert.end()
    }
  })

  consumerTests.test('Test Consumer::constructor - defaults', (assert) => {
    const config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageAsJSON: true,
        sync: true,
        syncConcurrency: 1,
        consumeTimeout: 1000,
        deserializeFn: 123
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger
    }
    const ConsumerSpy = Sinon.spy(Consumer.prototype, 'constructor')
    const c = new ConsumerSpy(topicsList, config)
    assert.ok(c, 'Consumer instance created')
    assert.ok(ConsumerSpy.calledOnce, 'Consumer constructor called once')
    ConsumerSpy.restore()
    assert.end()
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
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
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
      logger.error(error)
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
        logger.error(err)
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
        logger.error(error)
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

  consumerTests.test('Test Consumer::getMetadataSync', async (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(async result => {
      assert.ok(result, 'connection result received')
      c.getMetadataSync(null).then(metadata => {
        assert.ok(metadata, 'metadata object exists')
        assert.deepEqual(metadata, KafkaStubs.metadataSampleStub, 'metadata objects match')
        assert.end()
      }).catch(error => {
        assert.fail(error)
        assert.end()
      })
    })
  })

  consumerTests.test('Test Consumer::isConnected', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      const isConnected = c.isConnected()
      assert.ok(isConnected, 'isConnected result exists')
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::connectedTime', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      const connectedTime = c.connectedTime()
      assert.equal(connectedTime, 0, 'connectedTime result exists')
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

  consumerTests.test('Test Consumer::getOptions - returns the configured options', (assert) => {
    const c = new Consumer(topicsList, config)
    assert.deepEqual(c.getOptions(), config.options, 'getOptions returns a copy of the consumer options')
    assert.end()
  })

  consumerTests.test('Test Consumer::getOptions - reflects commitStrategy when set', (assert) => {
    const modifiedConfig = { ...config, options: { ...config.options, commitStrategy: 'async' } }
    const c = new Consumer(topicsList, modifiedConfig)
    assert.equal(c.getOptions().commitStrategy, 'async', 'getOptions exposes commitStrategy')
    assert.end()
  })

  consumerTests.test('Test Consumer::getOffsetCommitErrorCount - defaults to 0', (assert) => {
    const c = new Consumer(topicsList, config)
    assert.equal(c.getOffsetCommitErrorCount(), 0, 'no offset commit errors observed yet')
    assert.end()
  })

  consumerTests.test('Test Consumer::offset.commit - increments error counter, logs and emits offset.commit.error on failure', (assert) => {
    assert.plan(4)
    const modifiedConfig = { ...config, rdkafkaConf: { ...config.rdkafkaConf, offset_commit_cb: true } }
    const c = new Consumer(topicsList, modifiedConfig)
    const commitError = new Error('commit failed')
    const topicPartitions = [{ topic: 'test', partition: 0, offset: 5 }]

    c.on('offset.commit.error', (err, tps) => {
      assert.equal(err, commitError, 'offset.commit.error emitted with the underlying error')
      assert.equal(tps, topicPartitions, 'offset.commit.error emitted with topic partitions')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c._consumer.emit('offset.commit', commitError, topicPartitions)
      assert.equal(c.getOffsetCommitErrorCount(), 1, 'offset commit error counter incremented')
    }).catch(error => {
      assert.fail(error.message)
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::offset.commit - success does not increment error counter', (assert) => {
    const modifiedConfig = { ...config, rdkafkaConf: { ...config.rdkafkaConf, offset_commit_cb: true } }
    const c = new Consumer(topicsList, modifiedConfig)

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c._consumer.emit('offset.commit', null, [{ topic: 'test', partition: 0, offset: 5 }])
      assert.equal(c.getOffsetCommitErrorCount(), 0, 'offset commit error counter not incremented on success')
      assert.end()
    }).catch(error => {
      assert.fail(error.message)
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::offset.commit - listener not attached when offset_commit_cb is not set', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      assert.equal(c._consumer.listenerCount('offset.commit'), 0, 'no offset.commit listener registered by default')
      assert.end()
    }).catch(error => {
      assert.fail(error.message)
      assert.end()
    })
  })

  consumerTests.test('Test Consumer::event.stats - records group stats metrics and health, and clears them on disconnect', async (assert) => {
    Metrics.setup({ prefix: '', defaultMetrics: false })
    const group = 'stats-metrics-test'
    const modifiedConfig = { ...config, rdkafkaConf: { ...config.rdkafkaConf, 'group.id': group, 'statistics.interval.ms': 1 } }
    const c = new Consumer(topicsList, modifiedConfig)
    const assignedFor = async () => (await Metrics.getDefaultRegister().getSingleMetric('kafka_consumer_assigned_partitions').get())
      .values.filter(v => v.labels.consumerGroup === group)

    try {
      await c.connect()
      c._consumer.emit('event.stats', {
        message: JSON.stringify({
          brokers: { 1: { state: 'UP' } },
          cgrp: { assignment_size: 3, rebalance_cnt: 1 },
          topics: { test: { partitions: { 0: { consumer_lag: 4 } } } }
        })
      })
      assert.equal(c.isEventStatsConnectionHealthy(), true, 'health still tracked from the same stats')
      assert.deepEqual((await assignedFor()).map(v => v.value), [3], 'assigned partitions recorded for the consumer group')

      c.disconnect()
      assert.equal((await assignedFor()).length, 0, 'series removed on disconnect')
    } catch (err) {
      assert.fail(err.message)
    }
    assert.end()
  })

  consumerTests.test('Test Consumer::event.stats - no stats observer when statistics are disabled', async (assert) => {
    const c = new Consumer(topicsList, config)
    try {
      await c.connect()
      assert.equal(c._statsObserver, undefined, 'no observer created')
      assert.doesNotThrow(() => c.disconnect(), 'disconnect without observer')
    } catch (err) {
      assert.fail(err.message)
    }
    assert.end()
  })

  consumerTests.test('Test Consumer::_observeTransit - records every message in a batch, and single messages', async (assert) => {
    Metrics.setup({ prefix: '', defaultMetrics: false })
    const group = 'transit-test'
    const c = new Consumer(topicsList, { ...config, rdkafkaConf: { ...config.rdkafkaConf, 'group.id': group } })
    const transitCount = async (topic) => {
      const values = (await Metrics.getDefaultRegister().getSingleMetric('kafka_transit_seconds').get()).values
      const found = values.find(v => v.metricName === 'kafka_transit_seconds_count' && v.labels.topic === topic && v.labels.consumerGroup === group)
      return found ? found.value : 0
    }
    const now = Date.now()

    c._observeTransit([{ topic: 'batch-topic', timestamp: now - 20 }, { topic: 'batch-topic', timestamp: now - 10 }], now - 5)
    c._observeTransit({ topic: 'single-topic', timestamp: now - 20 }, now - 5)
    c._observeTransit({ topic: 'skipped-topic', timestamp: now - 20 }, undefined)
    c._observeTransit(null, now)

    assert.equal(await transitCount('batch-topic'), 2, 'each batch message observed')
    assert.equal(await transitCount('single-topic'), 1, 'single message observed')
    assert.equal(await transitCount('skipped-topic'), 0, 'skipped without a fetch time')
    assert.end()
  })

  consumerTests.test('Test Consumer::consumeOnce - Not Implemented - default params', (assert) => {
    const c = new Consumer(topicsList, config)
    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consumeOnce()
      } catch (error) {
        logger.error(error)
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
              logger.error(error)
              reject(error)
            }
            resolve(true)
          })
        })
      } catch (error) {
        logger.error(error)
        assert.equals(error.message.toString(), 'Not implemented')
        assert.end()
      }
    })
  })

  consumerTests.test('Test Consumer::consume - defaults', (assert) => {
    let messageReceived = false

    const c = new Consumer(topicsList, config)

    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    c.on('error', error => {
      logger.error(`error: ${error}`)
    })

    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    let consumeCount = 0
    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`OMG - ${error}`)
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
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
        })
      })

      c.consume((error, message) => {
        return new Promise(() => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          throw new Error(errorMessageThrown)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          reject(errorMessageRejected)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    let consumeCount = 0
    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`onError: ${error}`)
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
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow consumer')
          resolve(true)
        })
      })

      c.consume((error, message) => {
        return new Promise(() => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          throw new Error(errorMessageThrown)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
          assert.ok(true, 'Message processed by the flow to throw exception consumer')
          reject(errorMessageRejected)
        })
      })

      c.consume((error, message) => {
        return new Promise((resolve) => {
          consumeCount = consumeCount + 1
          logger.info(`consume::callback[recursiveCount=${consumeCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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

  consumerTests.test('Test Consumer::consume recursive sync=true, messageAsJson=true, syncSingleMessage=true', (assert) => {
    config = {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        syncSingleMessage: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'group.id': 'kafka-test',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': false
      },
      topicConf: {},
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
              assert.ok(!Array.isArray(message), 'single message received')
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.on('batch', messages => {
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
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
              logger.error(error)
              reject(error)
            }
            if (message) { // check if there is a valid message comming back
              logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    let recursiveCount = 0
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`OMG - ${error}`)
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
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          logger.info(`consume::callback[recursiveCount=${recursiveCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    let recursiveCount = 0
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`OMG - ${error}`)
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
      logger.debug(`onBatch: ${JSON.stringify(messages)}`)
      assert.ok(messages, 'on Batch event received')
      assert.ok(Array.isArray(messages), 'batch of messages received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          recursiveCount = recursiveCount + 1
          logger.info(`consume::callback[recursiveCount=${recursiveCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consume()
      } catch (error) {
        logger.error(error)
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
      logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let pollCount = 0
    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`OMG - ${error}`)
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
          logger.info(`consume::callback[recursiveCount=${pollCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    const errorMessageThrown = 'this is an error thrown'
    const errorMessageRejected = 'this is an error rejected'

    const c = new Consumer(topicsList, config)

    let pollCount = 0
    let errorHandledThrown = false
    let errorHandledRejected = false
    let processedNextMessage = false
    c.on('error', error => {
      logger.debug(`OMG - ${error}`)
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
          logger.info(`consume::callback[recursiveCount=${pollCount}] ${error}, ${JSON.stringify(message)}`)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      try {
        c.consume()
      } catch (error) {
        logger.error(error)
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
      logger
    }

    const c = new Consumer(topicsList, config)

    // consume 'ready' event
    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })
    // consume 'message' event
    c.on('message', message => {
      logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`)
      assert.ok(message, 'on Message event received')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')

      c.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (error) {
            logger.error(error)
            reject(error)
          }
          if (message) { // check if there is a valid message comming back
            logger.info(`Message Received by callback function - ${JSON.stringify(message)}`)
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
      logger
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
    assert.plan(7)
    const c = new Consumer(topicsList, config)

    const discoCallback = (err) => {
      if (err) {
        logger.error(`Error received: ${err}`)
      }
    }

    // consume 'message' event
    c.on('error', error => {
      logger.error(error)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    c.on('event.throttle', arg => {
      assert.ok(arg, 'event.throttle')
    })

    c.on('event.stats', arg => {
      assert.ok(arg, 'event.stats')
    })

    c.on('disconnected', arg => {
      assert.ok(arg, 'disconnected')
    })

    // This should never be called!
    c.on('message', arg => {
      assert.fail(arg, 'data')
    })

    c.on('partition.eof', arg => {
      assert.ok(arg, 'partition.eof')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.disconnect(discoCallback())
    })
  })

  consumerTests.test('Test Consumer::connect - test KafkaConsumer events: event.log, event.error, error, stats enabled', tryCatchEndTest((assert) => {
    assert.plan(8)

    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const c = new Consumer(topicsList, modifiedConfig)

    const discoCallback = (err) => {
      if (err) {
        logger.error(`Error received: ${err}`)
      }
    }

    // consume 'message' event
    c.on('error', error => {
      logger.error(error)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    c.on('ready', arg => {
      logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    c.on('event.throttle', arg => {
      assert.ok(arg, 'event.throttle')
    })

    c.on('event.stats', arg => {
      assert.ok(arg, 'event.stats')
    })

    c.on('disconnected', arg => {
      assert.ok(arg, 'disconnected')
    })

    // This should never be called!
    c.on('message', arg => {
      assert.fail(arg, 'data')
    })

    c.on('partition.eof', arg => {
      assert.ok(arg, 'partition.eof')
    })

    c.connect().then(result => {
      assert.ok(result, 'connection result received')
      c.disconnect(discoCallback())
    })
  }))

  consumerTests.test('Test Consumer::isPollHealthy - returns true on first run (never polled)', (assert) => {
    const c = new Consumer(topicsList, config)
    // On first run, _lastPolledTime is set in constructor to Date.now()
    // So isPollHealthy should return true
    assert.equal(c.isPollHealthy(), true, 'isPollHealthy returns true on first run')
    assert.end()
  })

  consumerTests.test('Test Consumer::getLastPolledTime - returns last polled timestamp', (assert) => {
    const c = new Consumer(topicsList, config)
    const now = Date.now()
    c._lastPolledTime = now
    assert.equal(c.getLastPolledTime(), now, 'getLastPolledTime returns last polled timestamp')
    assert.end()
  })

  consumerTests.test('Test Consumer::isPollHealthy - returns true if within healthCheckPollInterval', (assert) => {
    const c = new Consumer(topicsList, config)
    const now = Date.now()
    c._lastPolledTime = now
    c._config.options.healthCheckPollInterval = 10000
    assert.equal(c.isPollHealthy(), true, 'isPollHealthy returns true if within healthCheckPollInterval')
    assert.end()
  })

  consumerTests.test('Test Consumer::isPollHealthy - returns false if outside healthCheckPollInterval', (assert) => {
    const c = new Consumer(topicsList, config)
    const now = Date.now() - 10001
    c._lastPolledTime = now
    c._config.options.healthCheckPollInterval = 10000
    assert.equal(c.isPollHealthy(), false, 'isPollHealthy returns false if outside healthCheckPollInterval')
    assert.end()
  })

  consumerTests.test('Test Consumer::isPollHealthy - returns false if _lastPolledTime is undefined', (assert) => {
    const c = new Consumer(topicsList, config)
    c._lastPolledTime = undefined
    assert.equal(c.isPollHealthy(), false, 'isPollHealthy returns false if _lastPolledTime is undefined')
    assert.end()
  })

  consumerTests.test('Test Consumer::isPollHealthy - returns false if _lastPolledTime is null', (assert) => {
    const c = new Consumer(topicsList, config)
    c._lastPolledTime = null
    assert.equal(c.isPollHealthy(), false, 'isPollHealthy returns false if _lastPolledTime is null')
    assert.end()
  })

  consumerTests.test('Test Consumer::isPollHealthy - works with custom healthCheckPollInterval', (assert) => {
    const c = new Consumer(topicsList, config)
    c._config.options.healthCheckPollInterval = 5000
    const now = Date.now()
    c._lastPolledTime = now - 4000
    assert.equal(c.isPollHealthy(), true, 'isPollHealthy returns true for custom interval')
    c._lastPolledTime = now - 6000
    assert.equal(c.isPollHealthy(), false, 'isPollHealthy returns false for custom interval')
    assert.end()
  })

  consumerTests.end()
})

Test('Consumer flow mode', (flowTests) => {
  let sandbox
  const topicsList = ['test']

  const flowConfig = (options = {}) => ({
    options: {
      mode: ConsumerEnums.CONSUMER_MODES.flow,
      messageCharset: 'utf8',
      messageAsJSON: true,
      sync: true,
      syncConcurrency: 1,
      consumeTimeout: 10,
      ...options
    },
    rdkafkaConf: {
      'group.id': 'kafka-test',
      'metadata.broker.list': 'localhost:9092',
      'enable.auto.commit': false
    },
    topicConf: {},
    logger
  })

  const kafkaMessage = (offset) => {
    const value = Buffer.from(JSON.stringify({ offset }), 'utf8')
    return { value, topic: 'test', partition: 0, offset, key: 'key', size: value.length, timestamp: Date.now() }
  }

  const deferred = () => {
    let resolve
    const promise = new Promise(_resolve => { resolve = _resolve })
    return { promise, resolve }
  }

  const flush = async () => {
    for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve))
  }

  // Connects and starts consuming, capturing the flow-mode callback so a test
  // can deliver messages (or a consume-loop error) one at a time.
  const consumeCapturingFlow = async (c, workDoneCb) => {
    await c.connect()
    let flowCb
    sandbox.stub(c._consumer, 'consume').callsFake(cb => { flowCb = cb })
    c.consume(workDoneCb)
    return flowCb
  }

  flowTests.beforeEach((test) => {
    sandbox = Sinon.createSandbox()
    sandbox.stub(Kafka, 'KafkaConsumer').callsFake(() => new KafkaStubs.KafkaConsumer())
    test.end()
  })

  flowTests.afterEach((test) => {
    sandbox.restore()
    test.end()
  })

  flowTests.test('connect - sets consumeLoopTimeoutDelay on the client only when configured', async (assert) => {
    const setDelay = sandbox.spy(KafkaStubs.KafkaConsumer.prototype, 'setDefaultConsumeLoopTimeoutDelay')

    await new Consumer(topicsList, flowConfig()).connect()
    assert.ok(setDelay.notCalled, 'not set when the option is absent')

    await new Consumer(topicsList, flowConfig({ consumeLoopTimeoutDelay: 1 })).connect()
    assert.ok(setDelay.calledOnceWithExactly(1), 'set to the configured value')

    await new Consumer(topicsList, flowConfig({ consumeLoopTimeoutDelay: 0 })).connect()
    assert.ok(setDelay.calledWithExactly(0), 'zero is a valid value')
    assert.end()
  })

  flowTests.test('consume - an unrecognised mode with sync=true runs the flow consumer and passes a single message', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ mode: undefined }))
    await c.connect()
    const received = deferred()
    c.consume(async (error, message) => {
      received.resolve({ error, message })
      return true
    })
    const { error, message } = await received.promise
    assert.notOk(error, 'no error')
    assert.notOk(Array.isArray(message), 'a single message, not a batch')
    assert.equal(typeof message.value, 'object', 'message value deserialized')
    assert.end()
  })

  flowTests.test('flowMaxQueueSize - pauses the assigned partitions when the queue fills, and resumes at half', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ flowMaxQueueSize: 4 }))
    const inFlight = []
    const flowCb = await consumeCapturingFlow(c, () => {
      const work = deferred()
      inFlight.push(work)
      return work.promise
    })
    const pause = sandbox.spy(c._consumer, 'pause')
    const resume = sandbox.spy(c._consumer, 'resume')
    const assigned = [{ topic: 'test', partition: 0 }]

    for (let offset = 1; offset <= 3; offset++) flowCb(null, kafkaMessage(offset))
    assert.ok(pause.notCalled, 'not paused below flowMaxQueueSize')
    for (let offset = 4; offset <= 5; offset++) flowCb(null, kafkaMessage(offset))
    assert.ok(pause.calledWithExactly(assigned), 'paused when the queue reached flowMaxQueueSize')
    assert.ok(c._flowPaused, 'consumer marked paused')
    const pauseCount = pause.callCount

    await flush()
    inFlight[0].resolve(true)
    await flush()
    inFlight[1].resolve(true)
    await flush()
    assert.ok(resume.notCalled, 'still paused while the queue is above half')

    inFlight[2].resolve(true)
    await flush()
    assert.ok(resume.calledWithExactly(assigned), 'resumed once the queue fell to half')
    assert.notOk(c._flowPaused, 'consumer marked resumed')

    inFlight[3].resolve(true)
    await flush()
    inFlight[4].resolve(true)
    await flush()
    assert.equal(pause.callCount, pauseCount, 'not paused again below the limit')
    assert.end()
  })

  flowTests.test('flowMaxQueueSize - partitions assigned while paused are paused again by the next message at the limit', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ flowMaxQueueSize: 2 }))
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    const pause = sandbox.spy(c._consumer, 'pause')

    for (let offset = 1; offset <= 3; offset++) flowCb(null, kafkaMessage(offset))
    assert.ok(c._flowPaused, 'consumer marked paused')
    const pauseCount = pause.callCount

    // A rebalance assigns a new, unpaused partition while the consumer is still marked paused
    const reassigned = [{ topic: 'test', partition: 0 }, { topic: 'test', partition: 1 }]
    sandbox.stub(c._consumer, 'assignments').returns(reassigned)
    flowCb(null, { ...kafkaMessage(1), partition: 1 })
    assert.equal(pause.callCount, pauseCount + 1, 'paused again')
    assert.ok(pause.lastCall.calledWithExactly(reassigned), 'newly assigned partition paused with the rest')
    assert.ok(c._flowPaused, 'consumer still marked paused')
    assert.end()
  })

  flowTests.test('flowMaxQueueSize - absent leaves the queue unbounded', async (assert) => {
    const c = new Consumer(topicsList, flowConfig())
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    const pause = sandbox.spy(c._consumer, 'pause')

    for (let offset = 1; offset <= 50; offset++) flowCb(null, kafkaMessage(offset))
    assert.ok(pause.notCalled, 'never paused')
    assert.end()
  })

  flowTests.test('flowMaxQueueSize - a failed pause is reported and retried on the next message', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ flowMaxQueueSize: 1 }))
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    const pause = sandbox.stub(c._consumer, 'pause')
    pause.onFirstCall().throws(new Error('pause failed'))
    const errors = []
    c.on('error', err => errors.push(err))

    flowCb(null, kafkaMessage(1))
    assert.equal(errors.length, 1, 'error emitted')
    assert.notOk(c._flowPaused, 'not marked paused')

    flowCb(null, kafkaMessage(2))
    assert.ok(pause.calledTwice, 'pause retried')
    assert.ok(c._flowPaused, 'marked paused after the retry succeeded')
    assert.end()
  })

  flowTests.test('flowMaxQueueSize - a failed resume is reported and the consumer stays paused', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ flowMaxQueueSize: 1 }))
    const inFlight = []
    const flowCb = await consumeCapturingFlow(c, () => {
      const work = deferred()
      inFlight.push(work)
      return work.promise
    })
    sandbox.stub(c._consumer, 'resume').throws(new Error('resume failed'))
    const errors = []
    c.on('error', err => errors.push(err))

    flowCb(null, kafkaMessage(1))
    await flush()
    inFlight[0].resolve(true)
    await flush()
    assert.ok(errors.length >= 1, 'error emitted')
    assert.ok(c._flowPaused, 'still marked paused')
    assert.end()
  })

  flowTests.test('isPollHealthy - an idle flow consumer is healthy whatever the time since the last message', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ healthCheckPollInterval: 1000 }))
    await consumeCapturingFlow(c, async () => true)
    c._lastPolledTime = Date.now() - 60000
    assert.ok(c.isPollHealthy(), 'healthy while idle')
    assert.end()
  })

  flowTests.test('isPollHealthy - a flow consumer with work pending is unhealthy when nothing completes within healthCheckPollInterval', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ healthCheckPollInterval: 1000 }))
    const work = deferred()
    const flowCb = await consumeCapturingFlow(c, () => work.promise)

    flowCb(null, kafkaMessage(1))
    await flush()
    assert.ok(c.isPollHealthy(), 'healthy at the start of the progress window')

    c._lastProgressTime = Date.now() - 2000
    assert.notOk(c.isPollHealthy(), 'unhealthy once the window has passed without progress')

    work.resolve(true)
    await flush()
    assert.ok(c.isPollHealthy(), 'healthy again after the message completed')
    assert.end()
  })

  flowTests.test('isPollHealthy - work arriving at an idle flow consumer starts a new progress window', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ healthCheckPollInterval: 1000 }))
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    c._lastProgressTime = Date.now() - 60000

    flowCb(null, kafkaMessage(1))
    assert.ok(c.isPollHealthy(), 'a long idle period does not count against new work')
    assert.end()
  })

  flowTests.test('isPollHealthy - a flow consumer is unhealthy once its consume loop has stopped', async (assert) => {
    const c = new Consumer(topicsList, flowConfig())
    const flowCb = await consumeCapturingFlow(c, async () => true)
    const errors = []
    c.on('error', err => errors.push(err))

    flowCb(new Error('consume loop terminated'))
    assert.equal(errors.length, 1, 'error emitted')
    assert.notOk(c.isPollHealthy(), 'unhealthy')
    assert.end()
  })

  flowTests.test('reconnect - a flow consumer stopped by an error is healthy and unpaused again after reconnecting and consuming', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ flowMaxQueueSize: 1, healthCheckPollInterval: 1000 }))
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    c.on('error', () => {})

    flowCb(null, kafkaMessage(1))
    assert.ok(c._flowPaused, 'paused before the failure')
    c._lastProgressTime = Date.now() - 60000
    flowCb(new Error('consume loop terminated'))
    assert.notOk(c.isPollHealthy(), 'unhealthy once the loop has stopped')

    c.disconnect()
    await c.connect()
    assert.notOk(c._flowPaused, 'new client starts unpaused')
    sandbox.stub(c._consumer, 'consume')
    c.consume(async () => true)
    assert.notOk(c._flowLoopStopped, 'new consume loop not marked stopped')
    assert.ok(c.isPollHealthy(), 'healthy again after reconnecting')
    assert.ok(Date.now() - c._lastProgressTime < 1000, 'progress window restarted')
    assert.end()
  })

  flowTests.test('isPollHealthy - a flow consumer without sync relies on the consume loop only', async (assert) => {
    const c = new Consumer(topicsList, flowConfig({ sync: false, healthCheckPollInterval: 1000 }))
    const flowCb = await consumeCapturingFlow(c, () => new Promise(() => {}))
    flowCb(null, kafkaMessage(1))
    c._lastPolledTime = Date.now() - 60000
    c._lastProgressTime = Date.now() - 60000
    assert.ok(c.isPollHealthy(), 'healthy while the loop runs')

    flowCb(new Error('consume loop terminated'))
    assert.notOk(c.isPollHealthy(), 'unhealthy once the loop has stopped')
    assert.end()
  })

  flowTests.test('isPollHealthy - poll and recursive consumers keep the time-since-last-poll check', async (assert) => {
    for (const mode of [ConsumerEnums.CONSUMER_MODES.poll, ConsumerEnums.CONSUMER_MODES.recursive]) {
      const c = new Consumer(topicsList, flowConfig({ mode, healthCheckPollInterval: 1000 }))
      c._activeMode = mode
      c._lastPolledTime = Date.now() - 2000
      assert.notOk(c.isPollHealthy(), `mode ${mode}: unhealthy when the last poll is too old`)
      c._lastPolledTime = Date.now()
      assert.ok(c.isPollHealthy(), `mode ${mode}: healthy after a recent poll`)
    }
    assert.end()
  })

  flowTests.end()
})
