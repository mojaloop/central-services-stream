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

'use strict'

const EventEmitter = require('node:events')
const Kafka = require('node-rdkafka')
const Logger = require('@mojaloop/central-services-logger')
const ErrorHandler = require('@mojaloop/central-services-error-handling')

const Protocol = require('./protocol')
const getConfig = require('./config')

const connectedClients = new Set()
require('async-exit-hook')(callback => Promise.allSettled(
  Array.from(connectedClients).map(client => new Promise(resolve => client.disconnect(resolve)))
).finally(callback))

const { context, propagation, trace, SpanKind, SpanStatusCode } = require('@opentelemetry/api')
const { SemConv } = require('../constants')

const tracer = trace.getTracer('kafka-producer') // think, if we need to change it to clientId

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
 * @property {string} pending - pending processing
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
 * let producer = new Producer({
 *  options: {
 *   {
 *     pollIntervalMs: 50,
 *     messageCharset: 'utf8'
 *     serializeFn: (object, opts) => {}, (optional)
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
 *     'queue.buffering.max.ms': 5,
 *     'batch.num.messages': 10000,
 *     'api.version.request': true,
 *     'dr_cb': true
 *   },
 *   topicConf: {
 *    'request.required.acks': -1
 *   }
 * })
 *
 *  * @fires Producer#ready
 *
 * @param {object} options - Key value pairs for mapping to the configuration
 * @param {object} config - Key value pairs for the configuration of the Producer with the following:
 * rdkafkaConf - specific rdkafka configurations [Refer to configuration doc]{@link https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md}
 * topicConf - topic configuration
 * logger - logger object that supports debug(), info(), verbose(), error() & silly()
 * pollIntervalMs - is the number that will be passed to setPollInterval() when connected. Default is 50
 * @extends EventEmitter
 * @constructor
 */
class Producer extends EventEmitter {
  constructor (config = {}) {
    super()
    config = getConfig(config)
    if (!config) {
      config = {}
    }
    if (!config.options) {
      config.options = {
        messageCharset: 'utf8'
      }
    }
    if (!config.options.messageCharset) {
      config.options.messageCharset = 'utf8'
    }
    if (!config.options.serializeFn) {
      const defaultSerializeFn = (message, opts) => {
        return Producer._createBuffer(message, opts.messageCharset)
      }
      config.options.serializeFn = defaultSerializeFn
    }
    if (!config.options.pollIntervalMs) {
      config.options.pollIntervalMs = 50
    }
    if (!config.rdkafkaConf) {
      config.rdkafkaConf = {
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'default-client',
        event_cb: true,
        'compression.codec': 'none',
        'retry.backoff.ms': 100,
        'message.send.max.retries': 2,
        'statistics.interval.ms': 0, // Enable event.stats event if value is greater than 0
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 0, // 5 is default
        'batch.num.messages': 100,
        'api.version.request': true,
        dr_cb: true
      }
    }
    if (!config?.rdkafkaConf['client.id']) {
      config.rdkafkaConf['client.id'] = 'default-client'
    }
    if (!config.topicConf) {
      config.topicConf = {
        'request.required.acks': -1 // default for ALL brokers!
      }
    }
    if (!config.logger) {
      config.logger = Logger
    }
    const { logger } = config
    Logger.isSillyEnabled && logger.silly('Producer::constructor() - start')
    this._config = config
    this._status = {}
    this._status.runningInProduceMode = false
    this._status.runningInProduceBatchMode = false
    Logger.isSillyEnabled && logger.silly('Producer::constructor() - end')
  }

  /**
   * Returns the Kafka version library and features
   * @returns object containing Kafka info on librdkafkaVersion, and features
   */
  version () {
    return {
      librdkafkaVersion: Kafka.librdkafkaVersion,
      features: Kafka.features
    }
  }

  /**
 * Returns the current connection status of the consumer
 *
 * @returns boolean
 */
  isConnected () {
    Logger.isSillyEnabled && this._config?.logger?.silly('Producer::isConnected()')
    return this._producer.isConnected()
  }

  /**
   * Returns the current connection time of the consumer
   *
   * @returns number
   */
  connectedTime () {
    Logger.isSillyEnabled && this._config?.logger?.silly('Producer::connectedTime()')
    return this._producer.connectedTime()
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
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Producer::connect() - start')
    const result = await new Promise((resolve, reject) => {
      if (this._config.options.sync) {
        this._producer = new Kafka.HighLevelProducer(this._config.rdkafkaConf, this._config.topicConf)
      } else {
        // NOTE: this is the old way of creating a producer, we should use the new one
        this._producer = new Kafka.Producer(this._config.rdkafkaConf, this._config.topicConf)
      }

      Logger.isSillyEnabled && this._producer.on('event.log', log => {
        Logger.isSillyEnabled && logger.silly(`Producer::onEventLog - ${JSON.stringify(log.message)}`)
      })

      this._producer.on('event.error', error => {
        Logger.isDebugEnabled && logger.debug(`Producer::onEventError - ${JSON.stringify(error)}`)
        super.emit('error', error)
      })

      this._producer.on('event.throttle', eventData => {
        Logger.isDebugEnabled && logger.debug(`Producer::onEventThrottle - ${JSON.stringify(eventData)}`)
        super.emit('event.throttle', eventData)
      })

      if (this._config.rdkafkaConf['statistics.interval.ms'] > 0) {
        this._producer.on('event.stats', (eventData) => {
          Logger.isSillyEnabled && logger.silly(`Producer::onEventStats - ${JSON.stringify(eventData)}`)
          super.emit('event.stats', eventData)
        })
      }

      this._producer.on('error', error => {
        Logger.isDebugEnabled && logger.debug(`Producer::onError - ${JSON.stringify(error)}`)
        super.emit('error', error)
      })

      this._config.rdkafkaConf.dr_cb && this._producer.on('delivery-report', (err, report) => {
        Logger.isDebugEnabled && logger.debug(`Producer::onDeliveryReport - ${JSON.stringify(report)}`)
        super.emit('delivery-report', err, report)
      })

      this._producer.on('disconnected', (metrics) => {
        connectedClients.delete(this)
        Logger.isDebugEnabled && logger.debug(`Producer::onDisconnected - ${JSON.stringify(metrics)}`)
        super.emit('disconnected', metrics)
      })

      this._producer.on('ready', (args) => {
        Logger.isDebugEnabled && logger.debug(`Producer::onReady - Native producer ready v. ${Kafka.librdkafkaVersion}, e. ${Kafka.features.join(', ')}.`)
        // Passing non-integer (including "undefined") to setPollInterval() may cause unexpected behaviour, which is hard to trace.
        if (!Number.isInteger(this._config.options.pollIntervalMs)) {
          return reject(new Error('pollIntervalMs should be integer'))
        }
        this._producer.setPollInterval(this._config.options.pollIntervalMs)
        const readyResponse = {
          ...args,
          ...this.version()
        }
        super.emit('ready', readyResponse)
        resolve(true)
      })

      Logger.isSillyEnabled && logger.silly('Producer Connecting..')
      this._producer.connect(null, (error, metadata) => {
        if (error) {
          super.emit('error', error)
          Logger.isSillyEnabled && logger.silly('Producer::connect() - end')
          return reject(error)
        }
        connectedClients.add(this)
        Logger.isSillyEnabled && logger.silly('Producer::connect() - metadata:')
        Logger.isSillyEnabled && logger.silly(metadata)
        resolve(true)
      })
    })
    await this._monitorLag()
    return result
  }

  static _createBuffer (str, encoding) {
    const bufferResponse = Buffer.from(JSON.stringify(str), encoding)
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
   * @property {Array<{ [key: string]: string | Buffer }>} [customHeaders] - optional list of headers
   *
   * @todo Validate messageProtocol
   * @todo Validate topicConf
   *
   *
   * @returns {boolean} or if failed {Error}
   */
  async sendMessage (messageProtocol, topicConf, customHeaders = []) {
    if (this._config.lagMonitor?.max && this.lag > this._config.lagMonitor.max) {
      throw ErrorHandler.CreateFSPIOPError(
        ErrorHandler.Enums.FSPIOPErrorCodes.SERVICE_CURRENTLY_UNAVAILABLE,
        `Topic ${this._config.lagMonitor.topic} lag ${this.lag} > ${this._config.lagMonitor.max}`
      )
    }
    const { logger } = this._config

    try {
      if (!this._producer) {
        throw new Error('You must call and await .connect() before trying to produce messages.')
      }
      if (this._producer._isConnecting) {
        Logger.isDebugEnabled && logger.debug('Producer::sendMessage() - still connecting')
      }
      const producedAt = Date.now()

      // Parse Message into Protocol format
      const parsedMessage = Protocol.parseMessage(messageProtocol)
      // Serialize Message
      const parsedMessageBuffer = this._config.options.serializeFn(parsedMessage, this._config.options)
      // Validate Serialized Message
      if (!parsedMessageBuffer || !(typeof parsedMessageBuffer === 'string' || Buffer.isBuffer(parsedMessageBuffer))) {
        throw new Error('message must be a string or an instance of Buffer.')
      }

      Logger.isDebugEnabled && logger.debug(`Producer::sendMessage() - start: ${JSON.stringify({
          topicName: topicConf.topicName,
          partition: topicConf.partition,
          key: topicConf.key
        })}`)

      Logger.isSillyEnabled && logger.silly(`Producer::sendMessage() - message: ${JSON.stringify(parsedMessage)}`)

      return this.#produceMessageWithTrace({
        topicConf, parsedMessageBuffer, producedAt, customHeaders
      })
      // todo: think, if it's better to wrap the whole sendMessage in a span
    } catch (err) {
      Logger.isDebugEnabled && logger.debug(err)
      throw err
      // think, if we need to reformat error, e.g.  throw ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  }

  /**
   * Disconnect producer
   *
   * Disconnects producer from the Kafka broker
   */
  disconnect (cb = () => {}) {
    if (this._consumer) {
      this._consumer.disconnect(() => {
        this._consumer = null
      })
    }
    if (this._monitorLagInterval) {
      clearInterval(this._monitorLagInterval)
      this._monitorLagInterval = null
    }
    if (this._producer) {
      this._producer.flush()
      this._producer.disconnect(cb)
      this._producer = null
    }
  }

  /**
   * Get client metadata.
   *
   * RDKAFKA:
   *
   * Note: using a <code>metadataOptions.topic</code> parameter has a potential side-effect.
   * A Topic object will be created, if it did not exist yet, with default options
   * and it will be cached by librdkafka.
   *
   * A subsequent call to create the topic object with specific options (e.g. <code>acks</code>) will return
   * the previous instance and the specific options will be silently ignored.
   *
   * To avoid this side effect, the topic object can be created with the expected options before requesting metadata,
   * or the metadata request can be performed for all topics (by omitting <code>metadataOptions.topic</code>).
   *
   * @param {object} metadataOptions - Metadata options to pass to the client.
   * @param {string} metadataOptions.topic - Topic string for which to fetch
   * metadata
   * @param {number} metadataOptions.timeout - Max time, in ms, to try to fetch
   * metadata before timing out. Defaults to 30,000 (30 seconds).
   * @param {Client~metadataCallback} metaDataCb - Callback to fire with the metadata.
   */
  getMetadata (metadataOptions, metaDataCb) {
    if (!metaDataCb || typeof metaDataCb !== 'function') {
      metaDataCb = () => {}
    }
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Producer::getMetadata() - start')
    this._producer.getMetadata(metadataOptions, metaDataCb)
    Logger.isSillyEnabled && logger.silly('Producer::getMetadata() - end')
  }

  /**
   * Get client metadata synchronously.
   * To avoid this side effect, the topic object can be created with the expected options before requesting metadata,
   * or the metadata request can be performed for all topics (by omitting <code>metadataOptions.topic</code>).
   *
   * @typedef metadataOptions
   * @property {string} topic - Topic string for which to fetch metadata
   * @property {number} timeout - Max time, in ms, to try to fetch metadata before timing out. Defaults to 30,000 (30 seconds).
   *
   * @param {metadataOptions} metadataOptions - Metadata options to pass to the client.
   * @returns {Promise<object>} - Returns the metadata object.
   */
  getMetadataSync (metadataOptions) {
    return new Promise((resolve, reject) => {
      const metaDatacCb = (error, metadata) => {
        if (error) reject(error)
        resolve(metadata)
      }
      const { logger } = this._config
      Logger.isSillyEnabled && logger.silly('Producer::getMetadataSync() - start')
      this._producer.getMetadata(metadataOptions, metaDatacCb)
      Logger.isSillyEnabled && logger.silly('Producer::getMetadataSync() - end')
    })
  }

  async _getLag (partitionIds) {
    if (this._getLagActive) return
    this._getLagActive = true
    try {
      const commits = new Promise((resolve, reject) => {
        this._consumer.committed(undefined, 5000, (err, commits) => {
          if (err) {
            reject(err)
          } else {
            resolve(commits)
          }
        })
      })

      const highOffset = await Promise.all(partitionIds.map(partitionId => new Promise((resolve, reject) => {
        this._producer.queryWatermarkOffsets(this._config.lagMonitor.topic, partitionId, (this._config.lagMonitor.timeout || 5) * 1000, (err, offsets) => {
          if (err) {
            reject(err)
          } else {
            resolve(offsets.highOffset)
          }
        })
      })))

      this.lag = highOffset.reduce((acc, high) => acc + high, 0) - (await commits).reduce((acc, commit) => acc + commit.offset, 0)
      Logger.isDebugEnabled && this._config.logger.debug(`Producer::_monitorLag() - topic ${this._config.lagMonitor.topic} ${this.lag} messages behind`)
    } catch (err) {
      Logger.isErrorEnabled && this._config.logger.error(`Producer::_getLag() - error: ${err}`)
      return
    } finally {
      this._getLagActive = false
    }
    return this.lag
  }

  async _monitorLag () {
    this.lag = 0
    if (!this._config.lagMonitor?.consumerGroup || !this._config.lagMonitor?.topic) return
    this._consumer = new Kafka.KafkaConsumer({
      'metadata.broker.list': this._config.rdkafkaConf['metadata.broker.list'],
      'group.id': this._config.lagMonitor.consumerGroup,
      'enable.auto.commit': false
    }, {
      'auto.offset.reset': 'earliest'
    })
    await new Promise((resolve, reject) => {
      this._consumer.connect({ topic: this._config.lagMonitor.topic }, (err, metadata) => {
        if (err) {
          reject(err)
        } else {
          const partitionIds = metadata.topics.find(topic => topic.name === this._config.lagMonitor.topic).partitions.map(partition => partition.id)
          this._consumer.assign(partitionIds.map(partition => ({ topic: this._config.lagMonitor.topic, partition })))
          this._monitorLagInterval = setInterval(this._getLag.bind(this, partitionIds), (this._config.lagMonitor.interval || 5) * 1000)
          resolve()
        }
      })
    })
  }

  async #produceMessage ({
    topicConf, parsedMessageBuffer, producedAt, headers
  }) {
    const { logger } = this._config
    return new Promise((resolve, reject) => {
      if (this._config.options.sync) {
        this._producer.produce(
          topicConf.topicName,
          topicConf.partition,
          parsedMessageBuffer,
          topicConf.key,
          producedAt,
          headers,
          (err, offset) => {
            // The offset if our acknowledgement level allows us to receive delivery offsets
            if (err) {
              Logger.isWarnEnabled && logger.warn(`Producer::produce() - error: ${err?.stack}`)
              reject(err)
            } else {
              Logger.isDebugEnabled && logger.debug(`Producer::produce() - delivery-callback offset=${offset}`)
              resolve(offset)
            }
          })
      } else {
        // NOTE: this is the old way of producing a message, we should use the new one
        this._producer.produce(topicConf.topicName, topicConf.partition, parsedMessageBuffer, topicConf.key, producedAt, topicConf.opaqueKey, headers)
        resolve(true)
      }
    })
  }

  async #produceMessageWithTrace ({
    topicConf, parsedMessageBuffer, producedAt, customHeaders = []
  }) {
    return new Promise((resolve, reject) => {
      tracer.startActiveSpan(`SEND:${topicConf.topicName}`, { kind: SpanKind.PRODUCER }, async (span) => {
        try {
          const tracingContext = {}
          propagation.inject(context.active(), tracingContext)
          const headers = [
            ...customHeaders,
            ...Object.entries(tracingContext).map(([k, v]) => ({ [k]: v }))
          ]
          Logger.isDebugEnabled && this._config.logger.debug(`Producer::headers: ${JSON.stringify(headers)}`)

          span.setAttributes({
            [SemConv.ATTR_MESSAGING_CLIENT_ID]: this._config.rdkafkaConf['client.id'],
            [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topicConf.topicName,
            [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'send',
            [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka',
            [SemConv.ATTR_SERVER_ADDRESS]: this._config.rdkafkaConf['metadata.broker.list']
            // think, if we need to add more attributes
          })

          const result = await this.#produceMessage({
            topicConf, parsedMessageBuffer, producedAt, headers
          })

          span.setStatus({ code: SpanStatusCode.OK })
          resolve(result)
        } catch (err) {
          span.setStatus({ code: SpanStatusCode.ERROR, message: err.message })
          span.recordException(err)
          reject(err)
        } finally {
          span.end()
        }
      })
    })
  }
}

module.exports = Producer
module.exports.ENUMS = ENUMS
