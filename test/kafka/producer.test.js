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
 * Kafka Producer
 * @module Producer
 */

const Test = require('tapes')(require('tape'))
const Producer = require('../../src/kafka').Producer
// const ProducerEnums = require('../../src/kafka').Producer.ENUMS
const Logger = require('../../src/logger')
const Kafka = require('node-rdkafka')
const Sinon = require('sinon')
const KafkaStubs = require('./KafkaStub')

Test('Producer test', (producerTests) => {
  let sandbox
  let config = {}

  // lets setup the tests
  producerTests.beforeEach((test) => {
    sandbox = Sinon.sandbox.create()
    config = {
      options: {
        pollIntervalMs: 100,
        messageCharset: 'utf8'
      },
      rdkafkaConf: {
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'default-client',
        'event_cb': true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 10,
        'queue.buffering.max.ms': 50,
        'batch.num.messages': 100,
        'api.version.request': true,
        'dr_cb': true
      },
      topicConf: {
        'request.required.acks': 1
      },
      logger: Logger
    }

    sandbox.stub(Kafka, 'Producer').callsFake(
      () => {
        return new KafkaStubs.KafkaProducer()
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
    var producer = new ProducerSpy(config)
    assert.ok(producer, 'Producer instance created')
    assert.ok(ProducerSpy.calledOnce, 'Producer constructor called once')
    assert.end()
  })

  producerTests.test('Test Producer::constructor null', (assert) => {
    try {
      var producer = new Producer(null)
      assert.ok(producer, 'Producer instance created')
      assert.end()
    } catch (error) {
      assert.fail(error.message, 'missing a config object')
      assert.end()
    }
  })

  producerTests.test('Test Producer::constructor null', (assert) => {
    try {
      var producer = new Producer()
      assert.ok(producer, 'Producer instance created')
      assert.end()
    } catch (error) {
      assert.fail(error.message, 'missing a config object')
      assert.end()
    }
  })

  producerTests.test('Test Producer::connect - with error on callBack', (assert) => {
    sandbox.stub(KafkaStubs.KafkaProducer.prototype, 'connect').callsFake(
      (err, info) => {
        if (err) {
        }
        info('error test test', null)
      }
    )

    assert.plan(2)
    var producer = new Producer(config)

    // consume 'message' event
    producer.on('error', error => {
      Logger.error(error)
      assert.ok(Sinon.match(error, 'error test test'), 'on Error event received')
    })

    producer.connect().then(result => {
    }).catch((error) => {
      assert.ok(Sinon.match(error, 'Unhandled "error" event. (error test test)'))
    })
  })

  producerTests.test('Test Producer::connect', (assert) => {
    assert.plan(2)
    var producer = new Producer(config)
    producer.on('ready', arg => {
      console.log(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(Sinon.match(arg, true), 'on Ready event received')
    })
    producer.connect().then(result => {
      assert.ok(Sinon.match(result, true))
      assert.end()
      producer.disconnect()
    })
  })

  producerTests.test('Test Producer::disconnect', (assert) => {
    var discoCallback = (err, metrics) => {
      if (err) {
        Logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    var producer = new Producer(config)
    producer.connect().then(() => {
      producer.disconnect(discoCallback)
    })
  })

  producerTests.test('Test Producer::disconnect', (assert) => {
    try {
      var producer = new Producer(config)
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
    var producer = new Producer(config)
    var discoCallback = (err, metrics) => {
      if (err) {
        Logger.error(err)
      }
      assert.equal(typeof metrics.connectionOpened, 'number')
      assert.end()
    }
    // produce 'ready' event
    producer.on('ready', arg => {
      console.log(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(Sinon.match(arg, true), 'on Ready event received')
    })

    producer.connect().then(result => {
      assert.ok(Sinon.match(result, true))

      producer.sendMessage({message: {test: 'test'}, from: 'testAccountSender', to: 'testAccountReceiver', type: 'application/json', pp: '', id: 'id', metadata: {}}, {topicName: 'test', key: '1234'}).then(results => {
        producer.disconnect(discoCallback)
      })
    })
  })

  producerTests.test('Test Producer::sendMessage producer null', (assert) => {
    var producer = new Producer(config)
    producer.sendMessage({
      message: {test: 'test'},
      from: 'testAccountSender',
      to: 'testAccountReceiver',
      type: 'application/json',
      pp: '',
      id: 'id',
      metadata: {}
    }, {topicName: 'test', key: '1234'}).then(results => {}).catch((e) => {
      assert.ok(e.message, 'You must call and await .connect() before trying to produce messages.')
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
    sandbox = Sinon.sandbox.create()

    config = {
      options: {
        pollIntervalMs: 100,
        messageCharset: 'utf8'
      },
      rdkafkaConf: {
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'default-client',
        'event_cb': true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 10,
        'queue.buffering.max.ms': 50,
        'batch.num.messages': 100,
        'api.version.request': true,
        'dr_cb': true
      },
      topicConf: {
        'request.required.acks': 1
      },
      logger: Logger
    }

    sandbox.stub(Kafka, 'Producer').callsFake(
      () => {
        var k = new KafkaStubs.KafkaProducerForEventTests()
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
    assert.plan(4)
    var producer = new Producer(config)
    var discoCallback = (err) => {
      if (err) {
        Logger.error(err)
      }
      assert.end()
    }
    // consume 'message' event
    producer.on('error', error => {
      Logger.error(error)
      assert.ok(Sinon.match(error, 'event.error') || Sinon.match(error, 'event'), 'on Error event received')
    })

    producer.on('ready', arg => {
      Logger.debug(`onReady: ${JSON.stringify(arg)}`)
      assert.ok(Sinon.match(arg, true), 'on Ready event received')
    })

    producer.connect().then(result => {
      assert.ok(Sinon.match(result, true))
      producer.disconnect(discoCallback())
    })
  })

  producerTests.end()
})
