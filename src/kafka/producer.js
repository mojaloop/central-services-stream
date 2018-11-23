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

'use strict'

const EventEmitter = require('events')
const Logger = require('../logger')
const Kafka = require('node-rdkafka')
const Protocol = require('./protocol')

/**
 * Producer ENUMs
 *
 * This ENUM is for the events for the produced message
 *
 * The HTTP method for the process
 *
 * This ENUM is for the HTTP method that called the producer
 *
 * @typedef {object} ENUMS~METHOD
 * @property {string} get
 * @property {string} post
 * @property {string} put
 * @property {string} delete
 */
const METHOD = {
  get: 'get',
  post: 'post',
  put: 'put',
  del: 'delete'
}
/**
 * The status of the process being posted to the topic
 *
 * This ENUM is for the STATUS of the message being produced
 *
 * @typedef {object} ENUMS~STATUS
 * @property {string} success - successful validation passed
 * @property {string} failed - failed validation
 * @property {string} pending - pending proccessing
 */
const STATUS = {
  success: 'success',
  failure: 'failed',
  pending: 'pending'
}
/**
 * ENUMS
 *
 * Global ENUMS object
 *
 * @typedef {object} ENUMS
 * @property {object} METHOD - This ENUM is for the METHOD
 * @property {object} STATUS - This ENUM is for the STATUS
 */
const
  ENUMS = {
    METHOD,
    STATUS
  }

/**
 * Ready event.
 *
 * @event Producer#ready
 * @type {object}
 * @property {string} - emits 'ready' on producer event ready
 */

/**
 * Producer class for adding messages to Kafka
 *
 * This is the main entry point for writing data to Kafka. You
 * configure this like you do any other client, with a global
 * configuration and default topic configuration.
 *
 * @example
 * var producer = new Producer({
 *  options: {
 *   {
 *     pollIntervalMs: 100,
 *     messageCharset: 'utf8'
 *   },
 *   rdkafkaConf: {
 *     'metadata.broker.list': 'localhost:9092',
 *     'client.id': 'default-client',
 *     'event_cb': true,
 *     'compression.codec': 'none',
 *     'retry.backoff.ms': 100,
 *     'message.send.max.retries': 2,
 *     'socket.keepalive.enable': true,
 *     'queue.buffering.max.messages': 10,
 *     'queue.buffering.max.ms': 50,
 *     'batch.num.messages': 10000,
 *     'api.version.request': true,
 *     'dr_cb': true
 *   },
 *   topicConf: {
 *    'request.required.acks': 1
 *   }
 * })
 *
 *  * @fires Producer#ready
 *
 * @param {object} options - Key value pairs for mapping to the configuration
 * @param {object} config - Key value pairs for the configuration of the Producer with the following:
 * rdkafkaConf - specific rdkafka configurations [Refer to configuration doc]{@link https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md}
 * topicConf - topic configuration
 * logger - logger object that supports debug(), info(), verbose(), error() & silly()
 * @extends EventEmitter
 * @constructor
 */
class Producer extends EventEmitter {
  constructor (config = {}) {
    super()
    if (!config) {
      config = {}
    }
    if (!config.options) {
      config.options = {
        messageCharset: 'utf8'
      }
    }
    if (!config.rdkafkaConf) {
      config.rdkafkaConf = {
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
      }
    }
    if (!config.topicConf) {
      config.topicConf = {
        'request.required.acks': 1
      }
    }
    if (!config.logger) {
      config.logger = Logger
    }
    let {logger} = config
    logger.silly('Producer::constructor() - start')
    this._config = config
    this._status = {}
    this._status.runningInProduceMode = false
    this._status.runningInProduceBatchMode = false
    logger.silly('Producer::constructor() - end')
  }

  /**
   * Connect Producer
   *
   * @fires Producer#ready
   *
   * @async
   * Connects the producer to the Kafka broker.
   * @returns {Promise} - Returns a promise: resolved if successful, or rejection if connection failed
   */
  async connect () {
    let {logger} = this._config
    logger.silly('Producer::connect() - start')
    return new Promise((resolve, reject) => {
      this._producer = new Kafka.Producer(this._config.rdkafkaConf, this._config.topicConf)

      this._producer.on('event.log', log => {
        logger.silly(log.message)
      })

      this._producer.on('event.error', error => {
        super.emit('error', error)
      })

      this._producer.on('error', error => {
        super.emit('error', error)
      })

      this._producer.on('delivery-report', (report) => {
        logger.debug('DeliveryReport: ' + JSON.stringify(report))
      })

      this._producer.on('disconnected', () => {
        logger.warn('Disconnected.')
      })

      this._producer.on('ready', () => {
        logger.silly(`Native producer ready v. ${Kafka.librdkafkaVersion}, e. ${Kafka.features.join(', ')}.`)
        this._producer.poll()
        super.emit('ready')
        resolve(true)
      })

      logger.silly('Connecting..')
      this._producer.connect(null, (error, metadata) => {
        if (error) {
          super.emit('error', error)
          logger.silly('Consumer::connect() - end')
          return reject(error)
        }
        // this.subscribe()
        logger.silly('Consumer metadata:')
        logger.silly(metadata)
        resolve(true)
      })
    })
  }

  _createBuffer (str, encoding) {
    var bufferResponse = Buffer.from(JSON.stringify(str), encoding)
    return bufferResponse
  }

  /**
   * @async
   * produces a kafka message to a certain topic
   * @typedef {object} messageProtocol, contains message related data to be converted into a LIME protocol message
   * @property {object} content - value object for the message
   * @property {string} id - unique identifier for message
   * @property {string} from - uri of the initiating fsp
   * @property {string} to - uri of the receiving fsp
   * @property {object} metadata -  data relevant to the context of the message
   * @property {string} type - MIME declaration of the content type of the message
   * @property {string} pp - Optional for the sender, when is considered the identity of the session. Is mandatory in the destination if the identity of the originator is different of the identity of the from property.
   *
   *
   * @typedef {object} topicConf - contains Kafka topic related data
   * @property {*} opaqueKey - optional opaque token, which gets passed along to your delivery reports
   * @property {string} topicName - name of the topic to produce to
   * @property {string} key - optional message key
   * @property {number} partition - optional partition to produce to
   *
   * @todo Validate messageProtocol
   * @todo Validate topicConf
   *
   * @returns {boolean} or if failed {Error}
   */
  async sendMessage (messageProtocol, topicConf) {
    try {
      if (!this._producer) {
        throw new Error('You must call and await .connect() before trying to produce messages.')
      }
      if (this._producer._isConnecting) {
        this._config.logger.debug('still connecting')
      }
      var parsedMessage = Protocol.parseMessage(messageProtocol)
      const parsedMessageBuffer = this._createBuffer(parsedMessage, this._config.options.messageCharset)
      if (!parsedMessageBuffer || !(typeof parsedMessageBuffer === 'string' || Buffer.isBuffer(parsedMessageBuffer))) {
        throw new Error('message must be a string or an instance of Buffer.')
      }
      this._config.logger.debug('Producer::send() - start %s', JSON.stringify({
        topicName: topicConf.topicName,
        partition: topicConf.partition,
        key: topicConf.key
      }))
      const producedAt = Date.now()
      await this._producer.produce(topicConf.topicName, topicConf.partition, parsedMessageBuffer, topicConf.key, producedAt, topicConf.opaqueKey)
      return true
    } catch (e) {
      this._config.logger.debug(e)
      throw e
    }
  }

  // /**
  //  * @async
  //  * produces a kafka message to a certain topic
  //  * @param {string} topicName - name of the topic to produce to
  //  * @param {object} message - value object for the message
  //  * @param {string} key - optional message key
  //  * @param {string} from - uri of the initiating fsp
  //  * @param {string} to - uri of the receiving fsp
  //  * @param {object} metadata -  data relevant to the context of the message
  //  * @param {string} event - value from EVENT enum
  //  * @param {object} reason - if a failed event occurs the code and description will be populated
  //  * @param {string} type - MIME declaration of the content type of the message
  //  * @param {number} partition - optional partition to produce to
  //  * @param {string} pp - Optional for the sender, when is considered the identity of the session. Is mandatory in the destination if the identity of the originator is different of the identity of the from property.
  //  * @param {*} _opaqueKey - optional opaque token, which gets passed along to your delivery reports
  //  * @returns {Promise.<object>}
  //  */
  // async sendNotify (topicName, message, key, from, to, metadata, event, reason, type, pp, partition = 0, _opaqueKey = null) {
  //   try {
  //     if (!this._producer) {
  //       throw new Error('You must call and await .connect() before trying to produce messages.')
  //     }
  //     if (this._producer._isConnecting) {
  //       this._config.logger.debug('still connecting')
  //     }
  //     var parsedNotification = Protocol.parseNotify(from, to, key, message, metadata, event, reason, type, pp)
  //     // parsedNotification = Buffer.isBuffer(parsedNotification) ? parsedNotification : Buffer.from(JSON.stringify(parsedNotification))
  //     parsedNotification = this._createBuffer(parsedNotification, this._config.options.messageCharset)
  //     if (!parsedNotification || !(typeof parsedNotification === 'string' || Buffer.isBuffer(parsedNotification))) {
  //       throw new Error('message must be a string or an instance of Buffer.')
  //     }
  //     this._config.logger.debug('Producer::send() - start %s', JSON.stringify({
  //       topicName,
  //       partition,
  //       key
  //     }))
  //     const producedAt = Date.now()
  //     this._producer.produce(topicName, partition, parsedNotification, key, producedAt, _opaqueKey)
  //     return {
  //       key,
  //       message
  //     }
  //   } catch (e) {
  //     this._config.logger.debug(e)
  //     throw e
  //   }
  // }
  //
  // /**
  //  * @async
  //  * produces a kafka message to a certain topic
  //  * @param {string} topicName - name of the topic to produce to
  //  * @param {object} message - value object for the message
  //  * @param {string} key - optional message key
  //  * @param {string} from - uri of the initiating fsp
  //  * @param {string} to - uri of the receiving fsp
  //  * @param {object} metadata -  data relevant to the context of the message
  //  * @param {object} reason - if a failed event occurs the code and description will be populated
  //  * @param {string} method - value from METHOD enum
  //  * @param {string} type - MIME declaration of the content type of the message
  //  * @param {string} status - value from STATUS enum
  //  * @param {number} partition - optional partition to produce to
  //  * @param {string} pp - Optional for the sender, when is considered the identity of the session. Is mandatory in the destination if the identity of the originator is different of the identity of the from property.
  //  * @param {*} _opaqueKey - optional opaque token, which gets passed along to your delivery reports
  //  * @returns {Promise.<object>}
  //  */
  // async sendCommand (topicName, message, key, from, to, reason, method, metadata, status, type, pp, partition = 0, _opaqueKey = null) {
  //   try {
  //     if (!this._producer) {
  //       throw new Error('You must call and await .connect() before trying to produce messages.')
  //     }
  //     if (this._producer._isConnecting) {
  //       this._config.logger.debug('still connecting')
  //     }
  //     var parsedCommand = Protocol.parseCommand(from, to, key, message, reason, method, metadata, status, type, pp)
  //     // parsedCommand = Buffer.isBuffer(parsedCommand) ? parsedCommand : Buffer.from(JSON.stringify(parsedCommand))
  //     parsedCommand = this._createBuffer(parsedCommand, this._config.options.messageCharset)
  //     if (!parsedCommand || !(typeof parsedCommand === 'string' || Buffer.isBuffer(parsedCommand))) {
  //       throw new Error('message must be a string or an instance of Buffer.')
  //     }
  //     this._config.logger.debug('Producer::send() - start %s', JSON.stringify({
  //       topicName,
  //       partition,
  //       key
  //     }))
  //     const producedAt = Date.now()
  //     this._producer.produce(topicName, partition, parsedCommand, key, producedAt, _opaqueKey)
  //     return {
  //       key,
  //       message
  //     }
  //   } catch (e) {
  //     this._config.logger.debug(e)
  //     throw e
  //   }
  // }

  // publishHandler (event) {
  //   return async (eventMessage) => {
  //     const {topic, key, msg, id, to, from, metadata, pp, type} = eventMessage
  //     Logger.info('Kafka.publish.publishHandler:: start(%s, %s, %s)', topic, key, msg)
  //
  //     await this.sendMessage({message: msg, id, to, from, metadata, pp, type}, {topicName: topic, key}).then(results => {
  //       Logger.info(`Kafka.publish.publishHandler:: result:'${results}'`)
  //     })
  //   }
  // }

  /**
   * Disconnect producer
   *
   * Disconnects producer from the Kafka broker
   */
  disconnect (cb = () => {}) {
    if (this._producer) {
      this._producer.flush()
      this._producer.disconnect(cb)
    }
  }
}

module.exports = Producer
module.exports.ENUMS = ENUMS
