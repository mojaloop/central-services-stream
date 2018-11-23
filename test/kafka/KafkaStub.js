/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the 'License') and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

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

'use strict'

const EventEmitter = require('events')
const Logger = require('../../src/logger').Logger

const metadataSampleStub = {
  orig_broker_id: 1,
  orig_broker_name: 'stub-broker',
  brokers: [
    {
      id: 0,
      host: 'localhost',
      port: 9092
    }
  ],
  topics: [
    {
      name: 'test',
      partitions: [
        {
          id: 0,
          leader: 0,
          replicas: [1],
          isrs: [1]
        }
      ]
    }
  ]
}

const watermarkOffsetSampleStub = {
  high: 10,
  low: 0
}

const messageSampleStub = {
  value: null,
  topic: 'test',
  partition: 0,
  offset: 1,
  key: 'key',
  size: 0,
  timestamp: Date.now()
}

// KafkaClient Stub
class KafkaClient extends EventEmitter {
  // this function is called in every method so we can dummy it
  _dummyFunction () {
  }

  connect (err, info) {
    if (err) {
      Logger.error(err)
    }
    this.emit('ready', 'true')
    this.metrics = {}
    this.metrics.connectionOpened = Date.now()
    this.name = 'KafkaStub'
    info(null, this.metrics)
    this._dummyFunction()
  }

  disconnect (cb = (err, metrics) => {
    if (err) {
      Logger.error(err)
    }
  }) {
    cb(null, this.metrics)
    this.emit('disconnected', this.metrics)
    this._dummyFunction()
  }

  getMetadata (metadataOptions, cb = (err, metadata) => {
    if (err) {
      Logger.error(err)
    }
  }) {
    var metadataSample = {...metadataSampleStub}

    if (cb) {
      cb(null, metadataSample)
    }
    this._dummyFunction()
  }
}

// KafkaConsumer Stub
class KafkaConsumer extends KafkaClient {
  // this function is called in every method so we can dummy it
  _dummyFunction () {
    // this.emit('event.log', 'event.log')
    // this.emit('event.error', 'event.error')
    // this.emit('error', 'error')
  }

  testKafkaConsumersEmitters (event, message) {
    this.emit(event, message)
  }

  setDefaultConsumeTimeout (timeoutMs) {
    this._dummyFunction()
  }

  subscribe (topics) {
    this._dummyFunction()
    return topics
  }

  consume (number, cb) {
    if ((number && typeof number === 'number') || (number && cb)) {
      if (cb === undefined) {
        cb = function () {}
      } else if (typeof cb !== 'function') {
        throw new TypeError('Callback must be a function')
      }
    } else {
      // See https://github.com/Blizzard/node-rdkafka/issues/220
      // Docs specify just a callback can be provided but really we needed
      // a fallback to the number argument
      // @deprecated
      if (cb === undefined) {
        if (typeof number === 'function') {
          cb = number
        } else {
          cb = function () {}
        }
      }
    }

    const encoding = 'utf8'

    const bufferedMessage = Buffer.from(JSON.stringify({
      hello: 'world'
    }), encoding)

    const messageSample = {
      value: bufferedMessage,
      topic: 'test',
      partition: 0,
      offset: 1,
      key: 'key',
      size: bufferedMessage.length,
      timestamp: (new Date()).getTime()
    }

    if (number > 0) {
      var messageBatchSample = [0, 1, 2, 3, 4, 5, 6, 7, 9]

      messageBatchSample = messageBatchSample.map(index => {
        var newMessageSample = {...messageSample}
        newMessageSample.key = index
        newMessageSample.offset = index
        newMessageSample.timestamp = (new Date()).getTime()
        return newMessageSample
      })

      cb(null, messageBatchSample)
      this.emit('batch', messageBatchSample)
    }
    const copyOfMessageSample = {...messageSample}
    // var copyOfMessageSample = {}
    // Object.assign(copyOfMessageSample, messageSample)
    cb(null, messageSample)
    this.emit('data', copyOfMessageSample)
    this._dummyFunction()
  }

  commit (topicPartition) {
    this._dummyFunction()
    return topicPartition
  }

  commitMessage (msg) {
    this._dummyFunction()
    return msg
  }

  commitSync (topicPartition) {
    this._dummyFunction()
    return topicPartition
  }

  commitMessageSync (msg) {
    this._dummyFunction()
    return msg
  }

  getWatermarkOffsets (topic, partition) {
    var watermarkOffsetSample = {...watermarkOffsetSampleStub}
    this._dummyFunction()
    return watermarkOffsetSample
  }

  resume (topicPartitions) {
    this._dummyFunction()
  }

  pause (topicPartitions) {
    this._dummyFunction()
  }
}

// KafkaConsumer Stub
class KafkaConsumerForEventTests extends KafkaConsumer {
  connect (err, info) {
    super.connect(err, info)

    this.emit('event.error', 'event.error')
    this.emit('event.log', 'event.log')
    this.emit('error', 'error')

    info(null, this.metrics)
    this._dummyFunction()
  }
}

// KafkaConsumer Stub
class KafkaProducer extends KafkaClient {
  poll () {
  }

  flush () {
  }

  produce () {
  }
}

// KafkaProducerForEventTests Stub
class KafkaProducerForEventTests extends KafkaProducer {
  connect (err, info) {
    super.connect(err, info)

    this.emit('event.error', 'event.error')
    this.emit('error', 'error')
    this.emit('event.log', 'event.log')
    this.emit('delivery-report', 'delivery-report')
    info(null, this.metrics)
    this._dummyFunction()
  }
}

exports.metadataSampleStub = metadataSampleStub
exports.watermarkOffsetSampleStub = watermarkOffsetSampleStub
exports.messageSampleStub = messageSampleStub
exports.KafkaClient = KafkaClient
exports.KafkaConsumer = KafkaConsumer
exports.KafkaProducer = KafkaProducer
exports.KafkaConsumerForEventTests = KafkaConsumerForEventTests
exports.KafkaProducerForEventTests = KafkaProducerForEventTests
