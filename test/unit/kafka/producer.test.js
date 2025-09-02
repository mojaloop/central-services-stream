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

 * Lazola Lucas <lazola.lucas@modusbox.com>
 * Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 * Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/

/**
 * Kafka Producer
 * @module Producer
 */

const Test = require('tapes')(require('tape'))
const Producer = require('../../../src/kafka').Producer
const logger = require('../../../src/lib/logger').logger
const Kafka = require('node-rdkafka')
const Sinon = require('sinon')
const KafkaStubs = require('./KafkaStub')

Test('Producer test', (producerTests) => {
  let sandbox
  let config = {}

  // lets setup the tests
  producerTests.beforeEach((test) => {
    sandbox = Sinon.createSandbox()
    sandbox.stub(logger, 'isErrorEnabled').value(true)
    sandbox.stub(logger, 'isDebugEnabled').value(true)
    sandbox.stub(logger, 'isSillyEnabled').value(true)

    config = {
      options: {
        pollIntervalMs: 100,
        messageCharset: 'utf8'
      },
      rdkafkaConf: {
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'default-client',
        event_cb: true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 10,
        'queue.buffering.max.ms': 50,
        'batch.num.messages': 100,
        'api.version.request': true,
        dr_cb: true
      },
      topicConf: {
        'request.required.acks': 1
      },
      logger
    }

    sandbox.stub(Kafka, 'Producer').callsFake(
      () => {
        return new KafkaStubs.KafkaProducer()
      }
    )

    sandbox.stub(Kafka, 'KafkaConsumer').callsFake(
      () => {
        return new KafkaStubs.KafkaConsumerForLagTests()
      }
    )

    sandbox.stub(Kafka, 'HighLevelProducer').callsFake(
      () => {
        return new KafkaStubs.KafkaSyncProducer()
      }
    )
    test.end()
  })

  // lets tear down the tests
  producerTests.afterEach((test) => {
    sandbox.restore()
    test.end()
  })

  producerTests.test('Test Producer::constructor', (assert) => {
    const ProducerSpy = Sinon.spy(Producer.prototype, 'constructor')
    const producer = new ProducerSpy(config)
    assert.ok(producer, 'Producer instance created')
    assert.ok(ProducerSpy.calledOnce, 'Producer constructor called once')
    ProducerSpy.restore()
    assert.end()
  })

  producerTests.test('Test Producer::constructor - defaults', (assert) => {
    const config = {
      options: {
        pollIntervalMs: 100,
        serializeFn: 123
      },
      rdkafkaConf: {
        'metadata.broker.list': 'localhost:9092',
        event_cb: true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 10,
        'queue.buffering.max.ms': 50,
        'batch.num.messages': 100,
        'api.version.request': true,
        dr_cb: true
      },
      topicConf: {
        'request.required.acks': 1
      },
      logger
    }
    const ProducerSpy = Sinon.spy(Producer.prototype, 'constructor')
    const producer = new ProducerSpy(config)
    assert.ok(producer, 'Producer instance created')
    assert.ok(ProducerSpy.calledOnce, 'Producer constructor called once')
    ProducerSpy.restore()
    assert.end()
  })

  producerTests.test('Test Producer::constructor null', (assert) => {
    try {
      const producer = new Producer(null)
      assert.ok(producer, 'Producer instance created')
      assert.end()
    } catch (error) {
      assert.fail(error.message, 'missing a config object')
      assert.end()
    }
  })

  producerTests.test('Test Producer::constructor null', (assert) => {
    try {
      const producer = new Producer()
      assert.ok(producer, 'Producer instance created')
      assert.end()
    } catch (error) {
      assert.fail(error.message, 'missing a config object')
      assert.end()
    }
  })

  producerTests.test('Test Producer::connect - with error on callBack', (assert) => {
    sandbox.stub(KafkaStubs.KafkaProducer.prototype, 'connect').callsFake(
      function (_, info) {
        info('error test test', null)
      }
    )

    assert.plan(2)
    const producer = new Producer(config)

    // consume 'message' event
    producer.on('error', error => {
      logger.error(error)
      assert.ok(Sinon.match(error, 'error test test'), 'on Error event received')
    })

    producer.connect().then(result => {
      logger.info(`connection result = ${result}`)
    }).catch((error) => {
      assert.ok(Sinon.match(error, 'Unhandled "error" event. (error test test)'))
    })
  })

  producerTests.test('Test Producer::connect', async (assert) => {
    assert.plan(2)

    const producer = new Producer(config)
    producer.on('ready', function (args) {
      logger.info(`onReady: ${JSON.stringify(args)}`)
      assert.ok(args, 'on Ready event received')
    })
    producer.connect().then(result => {
      assert.ok(result, 'connection result received')
      assert.end()
      producer.disconnect()
    })
  })

  producerTests.test('Test Producer::disconnect', (assert) => {
    const discoCallback = (err, metrics) => {
      if (err) {
        logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    const producer = new Producer(config)
    producer.connect().then(() => {
      producer.disconnect(discoCallback)
    })
  })

  producerTests.test('Test Producer::disconnect', (assert) => {
    try {
      const producer = new Producer(config)
      producer.disconnect()
      assert.ok(true)
      assert.end()
    } catch (e) {
      assert.fail(e)
      assert.end()
    }
  })

  producerTests.test('Test Producer::sendMessage', (assert) => {
    assert.plan(3)
    const producer = new Producer(config)
    const discoCallback = (err, metrics) => {
      if (err) {
        logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    // produce 'ready' event
    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.connect().then(result => {
      assert.ok(result, 'connection result received')

      producer.sendMessage({ message: { test: 'test' }, from: 'testAccountSender', to: 'testAccountReceiver', type: 'application/json', pp: '', id: 'id', metadata: {} }, { topicName: 'test', key: '1234' }).then(() => {
        producer.disconnect(discoCallback)
      })
    })
  })

  producerTests.test('Test Producer::sendMessage with maxLag', (assert) => {
    assert.plan(4)
    const producer = new Producer({ ...config, lagMonitor: { interval: 1, max: 4, consumerGroup: 'test', topic: 'test' } })
    const discoCallback = (err, metrics) => {
      if (err) {
        logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    // produce 'ready' event
    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.connect()
      .then(result => new Promise(resolve => setTimeout(() => resolve(result), 2000))) // wait 2 seconds for the lag interval to pass
      .then(result => {
        assert.ok(result, 'connection result received')

        producer.sendMessage({ message: { test: 'test' }, from: 'testAccountSender', to: 'testAccountReceiver', type: 'application/json', pp: '', id: 'id', metadata: {} }, { topicName: 'test', key: '1234' })
          .catch(e => {
            logger.error(e)
            assert.equal(e.httpStatusCode, 503, 'Max lag exceeded http status code 503')
          })
          .then(() => {
            producer.disconnect(discoCallback)
          })
      })
  })

  producerTests.test('Test Producer::sendMessage with error', (assert) => {
    assert.plan(3)
    const producer = new Producer({ ...config, lagMonitor: { interval: 1, max: 4, consumerGroup: 'test', topic: 'error' } })
    const discoCallback = (err, metrics) => {
      if (err) {
        logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    // produce 'ready' event
    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.connect()
      .then(result => new Promise(resolve => setTimeout(() => resolve(result), 2000))) // wait 2 seconds for the lag interval to pass
      .then(result => {
        assert.ok(result, 'connection result received')

        producer.sendMessage({ message: { test: 'test' }, from: 'testAccountSender', to: 'testAccountReceiver', type: 'application/json', pp: '', id: 'id', metadata: {} }, { topicName: 'error', key: '1234' })
          .then(() => {
            producer.disconnect(discoCallback)
          })
      })
  })

  producerTests.test('Test sync Producer::sendMessage', (assert) => {
    assert.plan(4)
    const syncConfig = { ...config }
    syncConfig.options.sync = true
    const producer = new Producer(syncConfig)
    const discoCallback = (err, metrics) => {
      if (err) {
        logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    // produce 'ready' event
    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.connect().then(result => {
      assert.ok(result, 'connection result received')

      producer.sendMessage({ message: { test: 'test' }, from: 'testAccountSender', to: 'testAccountReceiver', type: 'application/json', pp: '', id: 'id', metadata: {} }, { topicName: 'test', key: '1234' }).then((resolve) => {
        assert.equal(resolve, 1)
        producer.disconnect(discoCallback)
      })
    })
  })

  producerTests.test('Test Producer::sendMessage producer null', (assert) => {
    const producer = new Producer(config)
    producer.sendMessage({
      message: { test: 'test' },
      from: 'testAccountSender',
      to: 'testAccountReceiver',
      type: 'application/json',
      pp: '',
      id: 'id',
      metadata: {}
    }, { topicName: 'test', key: '1234' }).then(() => {}).catch((e) => {
      assert.ok(e.message, 'You must call and await .connect() before trying to produce messages.')
      assert.end()
    })
  })

  producerTests.test('Test Producer::sendMessage producer null', (assert) => {
    const producer = new Producer(config)
    producer.sendMessage({
      message: { test: 'test' },
      from: 'testAccountSender',
      to: 'testAccountReceiver',
      type: 'application/json',
      pp: '',
      id: 'id',
      metadata: {}
    }, { topicName: 'test', key: '1234' }).then(() => {}).catch((e) => {
      assert.ok(e.message, 'You must call and await .connect() before trying to produce messages.')
      assert.end()
    })
  })

  producerTests.test('Test Producer::getMetadata', (assert) => {
    const metaDatacCb = (error, metadata) => {
      if (error) {
        logger.error(error)
      }
      assert.ok(metadata, 'metadata object exists')
      assert.deepEqual(metadata, KafkaStubs.metadataSampleStub, 'metadata objects match')
      assert.end()
    }
    const p = new Producer(config)
    p.connect().then(result => {
      assert.ok(result, 'connection result received')
      p.getMetadata(null, metaDatacCb)
    })
  })

  producerTests.test('Test Producer::getMetadata - no callback function', (assert) => {
    const p = new Producer(config)
    p.connect().then(result => {
      assert.ok(result, 'connection result received')
      p.getMetadata(null)
      assert.end()
    })
  })

  producerTests.test('Test Producer::getMetadataSync', async (assert) => {
    const p = new Producer(config)
    p.connect().then(async result => {
      assert.ok(result, 'connection result received')
      p.getMetadataSync(null).then(metadata => {
        assert.ok(metadata, 'metadata object exists')
        assert.deepEqual(metadata, KafkaStubs.metadataSampleStub, 'metadata objects match')
        assert.end()
      }).catch(error => {
        assert.fail(error)
        assert.end()
      })
    })
  })

  producerTests.test('Test Consumer::isConnected', (assert) => {
    const p = new Producer(config)
    p.connect().then(result => {
      assert.ok(result, 'connection result received')
      const isConnected = p.isConnected()
      assert.ok(isConnected, 'isConnected result exists')
      assert.end()
    })
  })

  producerTests.test('Test Consumer::connectedTime', (assert) => {
    const p = new Producer(config)
    p.connect().then(result => {
      assert.ok(result, 'connection result received')
      const connectedTime = p.connectedTime()
      assert.equal(connectedTime, 0, 'connectedTime result exists')
      assert.end()
    })
  })

  producerTests.end()
})

Test('Producer test for KafkaProducer events', (producerTests) => {
  let sandbox
  // let clock
  let config = {}

  // lets setup the tests
  producerTests.beforeEach((test) => {
    sandbox = Sinon.createSandbox()
    sandbox.stub(logger, 'isErrorEnabled').value(true)
    sandbox.stub(logger, 'isDebugEnabled').value(true)
    sandbox.stub(logger, 'isSillyEnabled').value(true)

    config = {
      options: {
        pollIntervalMs: 100,
        messageCharset: 'utf8'
      },
      rdkafkaConf: {
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'default-client',
        event_cb: true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 10,
        'queue.buffering.max.ms': 50,
        'batch.num.messages': 100,
        'api.version.request': true,
        dr_cb: true
      },
      topicConf: {
        'request.required.acks': 1
      },
      logger
    }

    sandbox.stub(Kafka, 'Producer').callsFake(
      () => {
        const k = new KafkaStubs.KafkaProducerForEventTests()
        return k
      }
    )
    test.end()
  })

  // lets tear down the tests
  producerTests.afterEach((test) => {
    sandbox.restore()
    test.end()
  })

  producerTests.test('Test Producer::connect - test KafkaProducer events: event.log, event.error, error, deliver-report', (assert) => {
    assert.plan(7)
    const producer = new Producer(config)
    const discoCallback = (err) => {
      if (err) {
        logger.error(`Error received: ${err}`)
      }
    }
    // consume 'message' event
    producer.on('error', error => {
      logger.info(`onError: ${error}`)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.on('event.throttle', arg => {
      assert.ok(arg, 'event.throttle')
    })

    // this should never be hit
    producer.on('event.stats', arg => {
      assert.fail(arg, 'event.stats')
    })

    producer.on('delivery-report', arg => {
      assert.ok(arg, 'delivery-report')
    })

    producer.on('disconnected', arg => {
      assert.ok(arg, 'disconnected')
    })

    producer.connect().then(result => {
      assert.ok(result, 'connection result received')
      producer.disconnect(discoCallback())
    })
  })

  producerTests.test('Test Producer::connect - test KafkaProducer events: event.log, event.error, error, deliver-report, stats enabled', (assert) => {
    assert.plan(8)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    const discoCallback = (err) => {
      if (err) {
        logger.error(`Error received: ${err}`)
      }
    }
    // consume 'message' event
    producer.on('error', error => {
      logger.info(`onError: ${error}`)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    producer.on('ready', arg => {
      logger.info(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(arg, 'on Ready event received')
    })

    producer.on('event.throttle', arg => {
      assert.ok(arg, 'event.throttle')
    })

    producer.on('event.stats', arg => {
      assert.ok(arg, 'event.stats')
    })

    producer.on('delivery-report', arg => {
      assert.ok(arg, 'delivery-report')
    })

    producer.on('disconnected', arg => {
      assert.ok(arg, 'disconnected')
    })

    producer.connect().then(result => {
      assert.ok(result, 'connection result received')
      producer.disconnect(discoCallback())
    })
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy default', (assert) => {
    const producer = new Producer(config)
    assert.equal(producer.isEventStatsConnectionHealthy(), true, 'Default eventStatsConnectionHealthy should be true')
    assert.end()
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy after healthy event.stats', async (assert) => {
    assert.plan(1)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    producer.on('ready', () => {
      // Simulate healthy event.stats
      const healthyStats = {
        brokers: {
          1: { state: 'UP' },
          2: { state: 'UP' }
        }
      }
      producer._producer.emit('event.stats', healthyStats)
      assert.equal(producer.isEventStatsConnectionHealthy(), true, 'eventStatsConnectionHealthy should be true when all brokers are UP')
      producer.disconnect()
    })
    await producer.connect()
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy after unhealthy event.stats', async (assert) => {
    assert.plan(1)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    producer.on('ready', () => {
      // Simulate unhealthy event.stats
      const unhealthyStats = {
        brokers: {
          1: { state: 'UP' },
          2: { state: 'DOWN' }
        }
      }
      producer._producer.emit('event.stats', unhealthyStats)
      assert.equal(producer.isEventStatsConnectionHealthy(), false, 'eventStatsConnectionHealthy should be false when any broker is DOWN')
      producer.disconnect()
    })
    await producer.connect()
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy with malformed event.stats', async (assert) => {
    assert.plan(1)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    producer.on('ready', () => {
      // Simulate malformed event.stats (invalid JSON string)
      producer._producer.emit('event.stats', '{notjson')
      assert.equal(producer.isEventStatsConnectionHealthy(), false, 'eventStatsConnectionHealthy should be false on malformed stats')
      producer.disconnect()
    })
    await producer.connect()
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy with event.stats as string', async (assert) => {
    assert.plan(1)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    producer.on('ready', () => {
      // Simulate event.stats as JSON string
      const statsString = JSON.stringify({
        brokers: {
          1: { state: 'UP' },
          2: { state: 'UP' }
        }
      })
      producer._producer.emit('event.stats', statsString)
      assert.equal(producer.isEventStatsConnectionHealthy(), true, 'eventStatsConnectionHealthy should be true when all brokers are UP (string)')
      producer.disconnect()
    })
    await producer.connect()
  })

  producerTests.test('Test Producer::isEventStatsConnectionHealthy with event.stats missing brokers', async (assert) => {
    assert.plan(1)
    const modifiedConfig = { ...config }
    modifiedConfig.rdkafkaConf['statistics.interval.ms'] = 1

    const producer = new Producer(modifiedConfig)
    producer.on('ready', () => {
      // Simulate event.stats with no brokers property
      const stats = { foo: 'bar' }
      producer._producer.emit('event.stats', stats)
      assert.equal(producer.isEventStatsConnectionHealthy(), false, 'eventStatsConnectionHealthy should be false when brokers property is missing')
      producer.disconnect()
    })
    await producer.connect()
  })

  producerTests.test('Test Producer::connect - pollIntervalMs not integer', async (assert) => {
    assert.plan(1)
    const badConfig = { ...config }
    badConfig.options.pollIntervalMs = 'notAnInteger'
    const producer = new Producer(badConfig)
    try {
      await producer.connect()
      assert.fail('Should have thrown error for non-integer pollIntervalMs')
    } catch (error) {
      assert.equal(error.message, 'pollIntervalMs should be integer', 'Error thrown for non-integer pollIntervalMs')
    }
  })

  producerTests.test('Test Producer::sendMessage throws for invalid buffer', async (assert) => {
    assert.plan(1)
    const producer = new Producer(config)
    await producer.connect()
    // Override serializeFn to return an object (not string or Buffer)
    producer._config.options.serializeFn = () => ({ not: 'buffer' })
    try {
      await producer.sendMessage({
        message: { test: 'test' },
        from: 'testAccountSender',
        to: 'testAccountReceiver',
        type: 'application/json',
        pp: '',
        id: 'id',
        metadata: {}
      }, { topicName: 'test', key: '1234' })
      assert.fail('Should have thrown error for invalid buffer')
    } catch (e) {
      assert.equal(e.message, 'message must be a string or an instance of Buffer.', 'Error thrown for invalid buffer')
    }
    producer.disconnect()
    assert.end()
  })

  producerTests.end()
})
