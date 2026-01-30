/* eslint-disable indent */
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
 * Kafka Consumer
 * @module Consumer
 */

'use strict'

const EventEmitter = require('events')
const async = require('async')
const contextLogger = require('../lib/logger').logger
const Kafka = require('node-rdkafka')

const Protocol = require('./protocol')
const getConfig = require('./config')

const connectedClients = new Set()
require('async-exit-hook')(callback => Promise.allSettled(
  Array.from(connectedClients).map(client => new Promise(resolve => client.disconnect(resolve)))
).finally(callback))

const otel = require('./otel')
const { trackConnectionHealth } = require('./shared')

/**
 * Consumer ENUMs
 *
 * This ENUM is for the Consumer Modes of operation
 *
 * @typedef {object} ENUMS~CONSUMER_MODES
 * @property {string} flow - Flow processing messages one at a time as quick as possible
 * @property {string} poll - Poll flow processing batch of messages as per the poller frequency
 * @property {string} recursive - Recursive flow processing batch of messages as quick as possible
 *
 */
const CONSUMER_MODES = {
  flow: 0,
  poll: 1,
  recursive: 2
}

/**
 * ENUMS
 *
 * Global ENUMS object
 *
 * @typedef {object} ENUMS
 * @property {object} CONSUMER_MODES - This ENUM is for the Consumer Modes of operation
 */
const ENUMS = {
  CONSUMER_MODES
}

exports.ENUMS = ENUMS

/**
 * KafkaConsumer message.
 *
 * This is the representation of a message read from Kafka.
 *
 * @typedef {object} KafkaConsumer~Message
 * @property {buffer} value - the message buffer from Kafka.
 * @property {string} topic - the topic name
 * @property {number} partition - the partition on the topic the message was on
 * @property {number} offset - the offset of the message
 * @property {string} key - the message key
 * @property {number} size - message size, in bytes.
 * @property {number} timestamp - message timestamp
 */

/**
 * @typedef {object} Client~watermarkOffsets
 * @property {number} high - High (newest/end) offset
 * @property {number} low - Low (oldest/beginning) offset
 */

/**
 * Consumer Options
 *
 * The options that can be configured for the Consumer
 *
 * @typedef {object} Consumer~Options
 * @property {number} mode - @see ENUMS~CONSUMER_MODES. Defaults: CONSUMER_MODES.recursive
 * @property {number} batchSize - The batch size to be requested by the Kafka consumer. Defaults: 1
 * @property {number} pollFrequency - The polling frequency in milliseconds. Only applicable when mode = CONSUMER_MODES.poll. Defaults: 10
 * @property {number} recursiveTimeout - The timeout in milliseconds for the recursive processing method should timeout. Only applicable when mode = CONSUMER_MODES.recursive. Defaults: 100
 * @property {string} messageCharset - Parse processed message from Kafka into a string using this encoding character set. Defaults: utf8
 * @property {boolean} messageAsJSON - Parse processed message from Kafka into a JSON object. Defaults: true
 * @property {boolean} sync - Ensures that messages are processed in order via a single thread. This may impact performance. Defaults: false
 * @property {number} consumeTimeout - Set the default consume timeout (milliseconds) provided to RDKafka c++land. Defaults: 1000
 * @property {boolean} [disableOtelSpanAutoCreation] - Defines if kafka-stream lib should create OTel span automatically. Defaults: false
 *
 */

/**
 * Batch event.
 *
 * @event Consumer#batch
 * @type {object}
 * @property {object} messages - List of messages that were consumed in batch
 */

/**
 * Message event.
 *
 * @event Consumer#message
 * @type {object}
 * @property {object} message - Single message that was confusmed in any processing mode
 */

/**
 * Ready event.
 *
 * @event Consumer#ready
 * @type {object}
 * @property {object} message - Single message that was confusmed in any processing mode
 */

/**
 * Recursive event.
 *
 * For internal use only
 *
 * @event Consumer#recursive
 * @type {object}
 * @property {object} arg - rdkafka ready result
 */

/**
 * Consumer class for reading messages from Kafka
 *
 * This is the main entry point for reading data from Kafka. You
 * configure this like you do any other client, with a global
 * configuration and default topic configuration.
 *
 * Once you instantiate this object, connecting will open a socket.
 * Data will not be read until you tell the consumer what topics
 * you want to read from.
 *
 * @example
 * let consumer = new Consumer(['test1'], {
 *   rdkafka: {
 *     'group.id': 'kafka',
 *     'client.id': 'default-client',
 *     'metadata.broker.list': 'localhost:9092',
 *     'enable.auto.commit': false,
 *     'auto.commit.interval.ms': 100,
 *   },
 *   options: {
 *     batchSize: 1, // only applicable when mode=1, 2 (i.e. POLL, RECURSIVE)
 *     pollFrequency: 10, // only applicable for poll mode=1 (i.e. POLL)
 *     recursiveTimeout: 100, // only applicable when mode=2 (i.e. RECURSIVE)
 *     messageCharset: 'utf8',
 *     messageAsJSON: true,
 *     sync: false,
 *     syncConcurrency: 1, // only applicable when sync=true
 *     syncSingleMessage: false, // only applicable when sync=true, and only applicable when mode=2 (i.e. RECURSIVE)
 *     consumeTimeout: 1000
 *     deserializeFn: (object, opts) => {}, (optional)
 *   },
 *   topic: {}
 * })
 *
 * @fires Consumer#ready
 * @fires Consumer#message
 * @fires Consumer#batch
 * @fires Consumer#recursive
 *
 * @param {string[]} topics - List of topics that will be auto subscribed
 * @param {object} config - Key value pairs for the configuration of the Consumer with the followin:
 * options - consumer processing configuration, topic - Key value pairs to create a default. @see Consumer~Options
 * rdkafkaConf - specific rdkafka condfigurations [Refer to RDKAFKA configuration doc]{@link https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md}
 * topicConf - topic configuration [Refer to RDKAFKA configuration doc]{@link https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md#topic-configuration-properties}
 * logger - @mojaloop/central-services-logger contextLogger
 * @extends EventEmitter
 * @constructor
 */
class Consumer extends EventEmitter {
  constructor (topics = [], config = {}) {
    super()
    config = getConfig(config)
    if (!config.options) {
      config.options = {
        mode: CONSUMER_MODES.recursive,
        batchSize: 1, // only applicable when mode=1, 2 (i.e. POLL, RECURSIVE)
        pollFrequency: 10, // only applicable for poll mode=1 (i.e. POLL)
        recursiveTimeout: 100, // only applicable when mode=2 (i.e. RECURSIVE)
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: false,
        syncConcurrency: 1, // only applicable when sync=true
        syncSingleMessage: false, // only applicable when sync=true, and only applicable when mode=2 (i.e. RECURSIVE)
        consumeTimeout: 1000,
        disableOtelSpanAutoCreation: false
      }
    }
    if (!config.options.syncConcurrency) {
      config.options.syncConcurrency = 1
    }
    if (!config.options.messageCharset) {
      config.options.messageCharset = 'utf8'
    }
    if (!config.options.healthCheckPollInterval) {
      config.options.healthCheckPollInterval = 25000
    }
    if (!config.options.deserializeFn) {
      const defaultDeserializeFn = (buffer, opts) => {
        return Consumer._parseBuffer(buffer, opts.messageCharset, opts.messageAsJSON)
      }
      config.options.deserializeFn = defaultDeserializeFn
    }
    if (!config.rdkafkaConf) {
      config.rdkafkaConf = {
        'group.id': 'kafka',
        'client.id': 'default-client',
        'metadata.broker.list': 'localhost:9092',
        'enable.auto.commit': true,
        'statistics.interval.ms': 0 // Enable event.stats event if value is greater than 0
        // 'debug': 'all'
      }
    }
    if (!config?.rdkafkaConf['client.id']) {
      config.rdkafkaConf['client.id'] = 'default-client'
    }
    if (!config.topicConf) {
      config.topicConf = {}
    }
    if (!config.logger) {
      config.logger = contextLogger
    }

    const { logger } = config
    logger.silly('Consumer::constructor() - start')
    this._topics = topics
    this._config = config
    this._status = {}
    this._status.runningInConsumeOnceMode = false
    this._status.runningInConsumeMode = false
    this._status.running = false
    this._eventStatsConnectionHealthy = true
    this._lastPolledTime = Date.now()

    // setup default onReady emit handler
    super.on('ready', (...args) => {
      logger.debug(`Consumer::onReady() [topics: ${this._topics}]`, { args })
    })

    // setup default onError emit handler
    super.on('error', error => {
      logger.error(`Consumer::onError() [topics: ${this._topics}]`, error)
    })

    // setup default onDisconnect emit handler
    super.on('disconnected', (...metrics) => {
      logger.warn(`Consumer::onDisconnected() [topics: ${this._topics}]`, { metrics })
    })

    logger.silly('Consumer::constructor() - end')
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
   * Connect consumer
   *
   * @fires Consumer#ready
   *
   * Connects consumer to the Kafka brocker, and sets up the configured processing mode
   * @return {Promise} - Returns a promise: resolved if successful, or rejection if connection failed
   */
  connect () {
    const { logger } = this._config
    logger.silly('Consumer::connect() - start')

    return new Promise((resolve, reject) => {
      this._consumer = new Kafka.KafkaConsumer(this._config.rdkafkaConf, this._config.topicConf)

      this._consumer.setDefaultConsumeTimeout(this._config.options.consumeTimeout)

      this._consumer.on('warning', (...args) => {
        logger.warn('Consumer::onWarning - ', { args })
      })

      this._consumer.on('event.log', log => {
        logger.silly('Consumer::onEventLog - ', log.message)
      })

      this._consumer.on('event.error', error => {
        logger.error('Consumer::onEventError - ', error)
        super.emit('error', error)
      })

      this._consumer.on('event.throttle', eventData => {
        logger.warn('Consumer::onEventThrottle - ', eventData)
        super.emit('event.throttle', eventData)
      })

      if (this._config.rdkafkaConf['statistics.interval.ms'] > 0) {
        this._consumer.on('event.stats', (eventData) => {
          logger.silly('Consumer::onEventStats - ', eventData)
          // Use shared trackConnectionHealth to update health status
          this._eventStatsConnectionHealthy = trackConnectionHealth(eventData, logger)
          super.emit('event.stats', eventData)
        })
      }

      this._consumer.on('error', error => {
        logger.error('Consumer::onError: ', error)
        super.emit('error', error)
      })

      this._consumer.on('partition.eof', eof => {
        logger.debug('Consumer::onPartitionEof - ', { eof })
        super.emit('partition.eof', eof)
      })

      this._consumer.on('disconnected', (...metrics) => {
        connectedClients.delete(this)
        logger.warn('Consumer::onDisconnected - ', { metrics })
        super.emit('disconnected', metrics)
      })

      this._consumer.on('ready', args => {
        logger.verbose(`Consumer::onReady - node-rdkafka v${Kafka.librdkafkaVersion} ready - `, { args })
        this.subscribe()
        const readyResponse = {
          ...args,
          ...this.version()
        }
        super.emit('ready', readyResponse)
        this._status.running = this.isConnected()
        logger.silly('Consumer::connect() - end')
        resolve(true)
      })

      logger.silly('Consumer::connect() - connecting...')
      this._consumer.connect(null, (error, metadata) => {
        if (error) {
          logger.warn('Consumer::connect() - error: ', error)
          super.emit('error', error)
          return reject(error)
        }
        connectedClients.add(this)
        logger.verbose('Consumer::connect() - connected')
      })
    })
  }

  /**
   * Returns whether the last event.stats indicated a healthy connection.
   * @returns {boolean}
   */
  isEventStatsConnectionHealthy () {
    return this._eventStatsConnectionHealthy
  }

  /**
   * Returns the current connection status of the consumer
   *
   * @returns boolean
   */
  isConnected () {
    this._config?.logger?.silly('Consumer::isConnected()')
    return this._consumer.isConnected()
  }

  /**
   * Returns the current connection time of the consumer
   *
   * @returns number
   */
  connectedTime () {
    this._config?.logger?.silly('Consumer::connectedTime()')
    return this._consumer.connectedTime()
  }

  /**
   * Disconnect consumer
   *
   * Disconnects consumer from the Kafka broker
   */
  disconnect (cb = () => {}) {
    const { logger } = this._config
    logger.silly('Consumer::disconnect() - start')
    if (this._pollInterval) {
      clearInterval(this._pollInterval)
    }
    this._status.running = false
    this._consumer.disconnect(cb)
    logger.silly('Consumer::disconnect() - end')
  }

  /**
   * Subscribe
   *
   * Subscribes the consumer to the specified topics. If topics is null, then no action will be taken.
   * @param {object} topics - List of topics. Defaults: null
   */
  subscribe (topics = null) {
    const { logger } = this._config
    logger.silly('Consumer::subscribe() - start')
    if (topics) {
      this._topics = topics
    }

    if (this._topics) {
      logger.verbose(`Consumer::subscribe() - subscribing to [${this._topics}]`)
      this._consumer.subscribe(this._topics)
    }
    logger.debug('Consumer::subscribe() - end')
  }

  /**
   * This callback returns the message read from Kafka.
   *
   * @callback Consumer~workDoneCb
   * @param {Error} error - An error, if one occurred while reading
   * the data.
   * @param {object} messages - Either a list or a single message @see KafkaConsumer~Message
   * @returns {Promise} - Returns resolved on success, or rejections on failure
   */

  /**
   * Consume
   *
   * Consume messages from Kafka as per the configuration specified in the constructor.
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   */
  consume (workDoneCb) {
    const { logger } = this._config
    logger.silly('Consumer::consume() - start')

    if (!workDoneCb || typeof workDoneCb !== 'function') {
      workDoneCb = async () => {}
    }

    // setup queues to ensure sync processing of messages if options.sync is true
    if (this._config.options.sync) {
      this._syncQueue = async.queue((task, callbackDone) => {
        logger.silly(`Consumer::consume()::syncQueue.queue[${this._syncQueue?.length()}] - Sync Process - `, { task })
        const payload = this._config.options.mode === ENUMS.CONSUMER_MODES.flow
          ? task.message
          : task.messages

        const workProcessing = () => Promise.resolve(workDoneCb(task.error, payload))
          .then((result) => {
            callbackDone(task.error, result) // this marks the completion of the processing by the worker
          })
          .catch((err) => {
            logger.error(`Consumer::consume()::syncQueue.queue[${this._syncQueue?.length()}] - workDoneCb - error: `, err)
            super.emit('error', err)
            callbackDone(err)
          })

        const skipOtelSpan = this._config.options.disableOtelSpanAutoCreation || (payload.length > 1)
        if (skipOtelSpan) {
          logger.debug('OTel tracing logic can be implemented inside workDoneCb using otel.startConsumerTracingSpan')
          workProcessing()
        } else {
          const { executeInsideSpanContext } = otel.startConsumerTracingSpan(payload, this._config)
          executeInsideSpanContext(workProcessing)
        }
      }, this._config.options.syncConcurrency)

      // a callback function, invoked when queue is empty.
      this._syncQueue.drain(() => {
        try {
          this._consumer.resume(this._topics) // resume listening new messages from the Kafka consumer group
        } catch (err) {
          logger.error('Consumer::syncQueue.drain() - error resuming consumer: ', err)
          super.emit('error', err)
        }
      })
    }

    switch (this._config.options.mode) {
      case CONSUMER_MODES.poll:
        if (this._config.options.batchSize && typeof this._config.options.batchSize === 'number') {
          this._consumePoller(this._config.options.pollFrequency, this._config.options.batchSize, workDoneCb)
        } else {
          // throw error
          throw new Error('batchSize option is not valid - Select an integer greater then 0')
        }
        break
      case CONSUMER_MODES.recursive:
        if (this._config.options.batchSize && typeof this._config.options.batchSize === 'number') {
          super.on('recursive', (error) => {
            logger.silly('Consumer::consume() - onRecursive - start')
            if (error) {
              logger.error('Consumer::consume() - onRecursive - error: ', error)
            }
            if (this._status.running) {
              this._consumeRecursive(this._config.options.recursiveTimeout, this._config.options.batchSize, workDoneCb)
            } else {
              logger.debug(`Consumer::consume() - onRecursive - status.running=${this._status.running}`)
            }
            logger.silly('Consumer::consume() - onRecursive - end')
          })
          this._consumeRecursive(this._config.options.recursiveTimeout, this._config.options.batchSize, workDoneCb)
        } else {
          // throw error
          throw new Error('batchSize option is not valid - Select an integer greater then 0')
        }
        break
      case CONSUMER_MODES.flow:
        this._consumeFlow(workDoneCb)
        break
      default:
        this._consumeFlow(workDoneCb)
    }
    logger.verbose('Consumer::consume() - end')
  }

  /**
   * (Internal) Consume Poller
   *
   * This function will also emit the following events:
   * 1. message - event containing each message consumed
   * 2. batch - event containing the batch of messages
   *
   * @fires Consumer#message
   * @fires Consumer#batch
   *
   * Consume messages from in batches by polling in a specified frequency
   * @param {number} pollFrequency - The polling frequency in milliseconds. Only applicable when mode = CONSUMER_MODES.poll. Defaults: 10
   * @param {number} batchSize - The batch size to be requested by the Kafka consumer. Defaults: 1
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   */
  _consumePoller (pollFrequency = 10, batchSize, workDoneCb) {
    const { logger } = this._config
    this._pollInterval = setInterval(() => {
      // if (this._status.running) {
      this._consumer.consume(batchSize, (error, messages) => {
        this._updateLastPolledTime()
        if (error || !messages.length) {
          if (error) {
            super.emit('error', error)
            logger.error('Consumer::_consumerPoller() - ERROR - ', error)
          } else {
            logger.silly('Consumer::_consumerPoller() - POLL EMPTY PING')
          }
        } else {
          // lets transform the messages into the desired format
          messages.forEach(msg => {
            const parsedValue = this._config.options.deserializeFn(msg.value, this._config.options)
            msg.value = parsedValue
            super.emit('message', msg)
          })

          if (this._config.options.messageAsJSON) {
            logger.debug(`Consumer::_consumePoller() - messages[${messages.length}]: `, messages)
          } else {
            logger.debug(`Consumer::_consumePoller() - messages[${messages.length}]: `, messages)
          }

          if (this._config.options.sync) {
            this._syncQueue.push({ error, messages }, function (err) {
              if (err) {
                logger.error('Consumer::_consumePoller()::syncQueue.push - error: ', err)
              }
            })
          } else {
            // todo: think how to start tracing span here (each message in the batch should have its own span?)
            Promise.resolve(workDoneCb(error, messages))
              .then((response) => {
                logger.debug('Consumer::_consumePoller() - non-sync wokDoneCb response - ', response)
              })
              .catch((err) => {
                logger.error('Consumer::_consumePoller() - non-sync wokDoneCb response - ', err)
                super.emit('error', err)
              })
            super.emit('batch', messages)
          }
        }
      })
      // }
    }, pollFrequency)
  }

  /**
   * (Internal) Consume Recursively
   *
   * Consume messages from via a recursive call.
   *
   * This function will also emit the following events:
   * 1. message - event containing each message consumed
   * 2. batch - event containing the batch of messages
   * 3. recursive - event to recursively call the recursive function - for internal use only!
   *
   * @tutorial consumer
   *
   * @fires Consumer#message
   * @fires Consumer#batch
   * @fires Consumer#recursive
   *
   * @param {number} recursiveTimeout - The timeout in milliseconds for the recursive processing method should timeout. Only applicable when mode = CONSUMER_MODES.recursive. Defaults: 100
   * @param {number} batchSize - The batch size to be requested by the Kafka consumer. Defaults: 1
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   * @returns {boolean} - true when successful
   */
  _consumeRecursive (recursiveTimeout = 100, batchSize, workDoneCb) {
    const { logger } = this._config
    this._consumer.consume(batchSize, (error, messages) => {
      this._updateLastPolledTime()
      if (error || !messages.length) {
        if (error) {
          super.emit('error', error)
        }
        if (this._status.running) {
          return setTimeout(() => {
            super.emit('recursive', error, messages)
          }, recursiveTimeout)
        } else {
          return false
        }
      } else {
        // lets transform the messages into the desired format
        messages.forEach(msg => {
          const parsedValue = this._config.options.deserializeFn(msg.value, this._config.options)
          msg.value = parsedValue
          super.emit('message', msg)
        })

        if (this._config.options.messageAsJSON) {
          logger.silly(`Consumer::_consumerRecursive() - messages[${messages.length}]: `, messages)
        } else {
          logger.silly(`Consumer::_consumerRecursive() - messages[${messages.length}]: `, messages)
        }

        if (this._config.options.sync) {
          // lets process the messages in batches
          if (!this._config.options.syncSingleMessage) {
            this._syncQueue.push({ error, messages }, (error, result) => {
              if (error) {
                logger.error('Consumer::_consumerRecursive()::syncQueue.Batch.push - error: ', error)
              }
              logger.debug('Consumer::_consumerRecursive()::syncQueue.Batch.push - result: ', result)
              super.emit('recursive', error, messages)
            })
          } else {
            // lets process each message individually
            for (const [index, msg] of messages.entries()) {
              this._syncQueue.push({ error, messages: msg }, (error, result) => {
                if (error) {
                  logger.error('Consumer::_consumerRecursive()::syncQueue.Single.push - error: ', error)
                }
                logger.debug('Consumer::_consumerRecursive()::syncQueue.Single.push - result: ', result)
                // lets only emit the recursive event once we have processed all the messages
                if (index === messages.length - 1) {
                  super.emit('recursive', error, messages)
                }
              })
            }
          }
        } else {
          // todo: think how to start tracing span here (each message in the batch should have its own span?)
          Promise.resolve(workDoneCb(error, messages))
            .then((response) => {
              logger.debug('Consumer::_consumerRecursive() - non-sync wokDoneCb response - ', response)
              super.emit('recursive', error, messages)
            }).catch((err) => {
              logger.error('Consumer::_consumerRecursive() - non-sync wokDoneCb response - ', err)
              super.emit('recursive', error, messages)
              super.emit('error', err)
            })
        }
        super.emit('batch', messages)
        return true
      }
    })
  }

  /**
   * (Internal) Consume Flow
   *
   * Consume messages in a flow - one at a time - as quick as possible. If you require performance consider using either the poll or recursive modes which can consume messages in batches.
   *
   * This function will also emit the following events:
   * 1. message - event containing each message consumed
   *
   * @fires Consumer#message
   *
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   */
  _consumeFlow (workDoneCb) {
    const { logger } = this._config
    this._consumer.consume((error, message) => {
      this._updateLastPolledTime()
      if (error || !message) {
        if (error) {
          super.emit('error', error)
        }
      } else {
        const parsedValue = this._config.options.deserializeFn(message.value, this._config.options)
        message.value = parsedValue
        super.emit('message', message)

        if (this._config.options.messageAsJSON) {
          logger.silly('Consumer::_consumerFlow() - message: ', message)
        } else {
          logger.silly('Consumer::_consumerFlow() - message: ', message)
        }

        if (this._config.options.sync) {
          this._syncQueue.push({ error, message }, function (err) {
            if (err) { logger.error('Consumer::_consumerFlow()::syncQueue.push - error: ', err) }
          })
        } else {
          // todo: think how to start tracing span here (each message in the batch should have its own span?)
          Promise.resolve(workDoneCb(error, message))
            .then((response) => {
              logger.debug('Consumer::_consumerFlow() - non-sync wokDoneCb response - ', response)
            }).catch((err) => {
              logger.error('Consumer::_consumerFlow() - non-sync wokDoneCb response - ', err)
              super.emit('error', err)
            })
        }
        // super.emit('batch', message) // not applicable in flow mode since its one message at a time
      }
    })
  }

  /**
   * Consume Once (Not implemented)
   *
   * Consume a single message once and only once.
   *
   * @todo Implement method
   *
   * This function will also emit the following events:
   * 1. data - event containing each message consumed
   * @param {number} batchSize - The batch size to be requested by the Kafka consumer. Defaults: 1
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   * @returns {object} - single message that was consumed
   */
  consumeOnce (workDoneCb) {
    if (!workDoneCb || typeof workDoneCb !== 'function') {
      workDoneCb = () => {}
    }
    throw new Error('Not implemented')
  }

  /**
   * Commit topics partition
   *
   * @param {object} topicPartitions - List of topics that must be commited. If null, it will default to the topics list provided in the constructor. Defaults = null
   */
  commit (topicPartitions = null) {
    const { logger } = this._config
    logger.silly('Consumer::commit() - start')
    this._consumer.commit(topicPartitions)
    logger.silly('Consumer::commit() - end')
  }

  /**
   * Commit message
   *
   * @param {KafkaConsumer~Message} msg - Kafka message to be commited
   */
  commitMessage (msg) {
    const { logger } = this._config
    logger.silly('Consumer::commitMessage() - start')
    this._consumer.commitMessage(msg)
    logger.silly('Consumer::commitMessage() - end')
  }

  /**
   * Commit topics partition in sync mode
   *
   * @param {object} topicPartitions - List of topics that must be commited. If null, it will default to the topics list provided in the constructor. Defaults = null
   */
  commitSync (topicPartitions = null) {
    const { logger } = this._config
    logger.silly('Consumer::commitSync() - start')
    this._consumer.commitSync(topicPartitions)
    logger.silly('Consumer::commitSync() - end')
  }

  /**
   * Commit message in sync mode
   *
   * @param {KafkaConsumer~Message} msg - Kafka message to be commited
   */
  commitMessageSync (msg) {
    const { logger } = this._config
    logger.silly('Consumer::commitMessageSync() - start')
    this._consumer.commitMessageSync(msg)
    logger.silly('Consumer::commitMessageSync() - end')
  }

  /**
   * Get last known offsets from the client.
   *
   * RDKAFKA:
   *
   * The low offset is updated periodically (if statistics.interval.ms is set)
   * while the high offset is updated on each fetched message set from the
   * broker.
   *
   * If there is no cached offset (either low or high, or both), then this will
   * throw an error.
   *
   * @param {string} topic - Topic to recieve offsets from.
   * @param {number} partition - Partition of the provided topic to recieve offsets from
   * @return {Client~watermarkOffsets} - Returns an object with a high and low property, specifying
   * the high and low offsets for the topic partition
   */
  getWatermarkOffsets (topic, partition) {
    const { logger } = this._config
    logger.silly('Consumer::getWatermarkOffsets() - start')
    logger.silly('Consumer::getWatermarkOffsets() - end')
    return this._consumer.getWatermarkOffsets(topic, partition)
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
   * @typedef metadataOptions
   * @property {string} topic - Topic string for which to fetch metadata
   * @property {number} timeout - Max time, in ms, to try to fetch metadata before timing out. Defaults to 30,000 (30 seconds).
   *
   * @param {metadataOptions} metadataOptions - Metadata options to pass to the client.
   * @param {Client~metadataCallback} metaDatacCb - Callback to fire with the metadata, cb = (error, metadata) => {}.
   */
  getMetadata (metadataOptions, metaDatacCb) {
    if (!metaDatacCb || typeof metaDatacCb !== 'function') {
      metaDatacCb = () => {}
    }
    const { logger } = this._config
    logger.silly('Consumer::getMetadata() - start')
    this._consumer.getMetadata(metadataOptions, metaDatacCb)
    logger.silly('Consumer::getMetadata() - end')
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
      logger.silly('Consumer::getMetadataSync() - start')
      this._consumer.getMetadata(metadataOptions, metaDatacCb)
      logger.silly('Consumer::getMetadataSync() - end')
    })
  }

  static _parseBuffer (buffer, encoding, asJson) {
    return Protocol.parseValue(buffer, encoding, asJson)
  }

  /**
   * Tracks the last time the consumer polled Kafka.
   * Used for health checks.
   */
  _updateLastPolledTime () {
    this._lastPolledTime = Date.now()
  }

  /**
   * Returns the timestamp of the last poll.
   * @returns {number} - Unix timestamp in ms
   */
  getLastPolledTime () {
    return this._lastPolledTime
  }

  /**
   * Returns true if the time since last poll has not exceeded healthCheckPollInterval.
   * Uses config.options.healthCheckPollInterval if set, otherwise defaults to 25000 ms.
   * @returns {boolean}
   */
  isPollHealthy () {
    const lastPoll = this.getLastPolledTime()
    if (!lastPoll) return false
    const opts = this._config?.options || {}
    const healthCheckPollInterval = opts.healthCheckPollInterval
    const timeSinceLastPoll = Date.now() - lastPoll
    return timeSinceLastPoll <= healthCheckPollInterval
  }

  /* istanbul ignore next */
  async isHealthy (timeout = 5000) {
    try {
      const isConnected = this.isConnected()
      const isAssigned = this._consumer.assignments().length > 0 // not necessary for all topics to be assigned a partition
      const isPollHealthy = this.isPollHealthy()

      // Check metadata health for all topics
      const unhealthyTopics = []
      for (const topic of this._topics) {
        const metaData = await this.getMetadataSync({ topic, timeout })
        const topicHealthy = metaData.topics.some(t => t.name === topic)
        if (!topicHealthy) {
          unhealthyTopics.push(topic)
        }
      }
      const isTopicHealthy = unhealthyTopics.length === 0

      const isHealthy = isConnected && isAssigned && isPollHealthy && isTopicHealthy
      if (!isHealthy) {
        this._config.logger.warn(`consumer is NOT healthy  [topics: ${this._topics}]`, { isConnected, isPollHealthy, isTopicHealthy, isAssigned, unhealthyTopics })
      }
      return isHealthy
    } catch (err) {
      this._config.logger.error('consumer is NOT healthy due to error: ', err)
      return false
    }
  }
}

//
// class Stream extends Consumer {
//   constructor (consumerConfig = {
//     'group.id': 'kafka',
//     'metadata.broker.list': 'localhost:9092'
//   }, globalConfig, topicConfig
//   ) {
//     super(consumerConfig, globalConfig, topicConfig)
//
//     this._stream = this._consumer.createReadStream(globalConfig, topicConfig, {
//       topics: ['librdtesting-01']
//     })
//   }
//
//   // connect () {
//   //   this._consumer.connect()
//   // }
//
//   on (type, func) {
//     this._stream.on(type, func)
//   }
// }

// TODO: WRITE STREAM CONSUMER

module.exports = Consumer
module.exports.ENUMS = ENUMS
