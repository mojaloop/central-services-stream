/* eslint-disable indent */
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

const EventEmitter = require('events')
const async = require('async')
const Logger = require('@mojaloop/central-services-logger')
const Kafka = require('node-rdkafka')

const Protocol = require('./protocol')
const getConfig = require('./config')

const connectedClients = new Set()
require('async-exit-hook')(callback => Promise.allSettled(
  Array.from(connectedClients).map(client => new Promise(resolve => client.disconnect(resolve)))
).finally(callback))

const { context, propagation, trace, SpanKind, SpanStatusCode } = require('@opentelemetry/api')
const { SemConv, OTEL_HEADERS } = require('../constants')

const tracer = trace.getTracer('kafka')

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
 * @property {number} partition - the partition on the topic the
 * message was on
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
 * @param {object} topics - List of topics that will be auto subscribed
 * @param {object} config - Key value pairs for the configuration of the Consumer with the followin:
 * options - consumer processing configuration, topic - Key value pairs to create a default. @see Consumer~Options
 * rdkafkaConf - specific rdkafka condfigurations [Refer to RDKAFKA configuration doc]{@link https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md}
 * topicConf - topic configuration [Refer to RDKAFKA configuration doc]{@link https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md#topic-configuration-properties}
 * logger - logger object that supports debug(), info(), verbose() & silly()
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
        consumeTimeout: 1000
      }
    }
    if (!config.options.syncConcurrency) {
      config.options.syncConcurrency = 1
    }
    if (!config.options.messageCharset) {
      config.options.messageCharset = 'utf8'
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
      config.logger = Logger
    }

    const { logger } = config
    Logger.isSillyEnabled && logger.silly('Consumer::constructor() - start')
    this._topics = topics
    this._config = config
    this._status = {}
    this._status.runningInConsumeOnceMode = false
    this._status.runningInConsumeMode = false
    this._status.running = false

    // setup default onReady emit handler
    Logger.isDebugEnabled && super.on('ready', arg => {
      Logger.isDebugEnabled && logger.debug(`Consumer::onReady()[topics='${this._topics}'] - ${JSON.stringify(arg)}`)
    })

    // setup default onError emit handler
    Logger.isErrorEnabled && super.on('error', error => {
      Logger.isErrorEnabled && logger.error(`Consumer::onError()[topics='${this._topics}'] - ${error.stack || error})`)
    })
    Logger.isSillyEnabled && logger.silly('Consumer::constructor() - end')
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
    Logger.isSillyEnabled && logger.silly('Consumer::connect() - start')

    return new Promise((resolve, reject) => {
      this._consumer = new Kafka.KafkaConsumer(this._config.rdkafkaConf, this._config.topicConf)

      this._consumer.setDefaultConsumeTimeout(this._config.options.consumeTimeout)

      Logger.isDebugEnabled && this._consumer.on('warning', warn => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onWarning - ${JSON.stringify(warn)}`)
      })

      this._consumer.on('event.log', log => {
        Logger.isSillyEnabled && logger.silly(`Consumer::onEventLog - ${JSON.stringify(log.message)})`)
      })

      this._consumer.on('event.error', error => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onEventError  - ${JSON.stringify(error)}`)
        super.emit('error', error)
      })

      this._consumer.on('event.throttle', eventData => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onEventThrottle - ${JSON.stringify(eventData)}`)
        super.emit('event.throttle', eventData)
      })

      if (this._config.rdkafkaConf['statistics.interval.ms'] > 0) {
        this._consumer.on('event.stats', (eventData) => {
          Logger.isSillyEnabled && logger.silly(`Consumer::onEventStats - ${JSON.stringify(eventData)}`)
          super.emit('event.stats', eventData)
        })
      }

      this._consumer.on('error', error => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onError - ${JSON.stringify(error)}`)
        super.emit('error', error)
      })

      this._consumer.on('partition.eof', eof => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onPartitionEof - ${JSON.stringify(eof)}`)
        super.emit('partition.eof', eof)
      })

      this._consumer.on('disconnected', (metrics) => {
        connectedClients.delete(this)
        Logger.isDebugEnabled && logger.debug(`Consumer::onDisconnected - ${JSON.stringify(metrics)}`)
        super.emit('disconnected', metrics)
      })

      this._consumer.on('ready', args => {
        Logger.isDebugEnabled && logger.debug(`Consumer::onReady - node-rdkafka v${Kafka.librdkafkaVersion} ready - ${JSON.stringify(args)}`)
        this.subscribe()
        const readyResponse = {
          ...args,
          ...this.version()
        }
        super.emit('ready', readyResponse)
        this._status.running = this.isConnected()
        Logger.isSillyEnabled && logger.silly('Consumer::connect() - end')
        resolve(true)
      })

      Logger.isSillyEnabled && logger.silly('Consumer::connect() - Connecting...')
      this._consumer.connect(null, (error, metadata) => {
        if (error) {
          super.emit('error', error)
          Logger.isSillyEnabled && logger.silly('Consumer::connect() - end')
          return reject(error)
        }
        connectedClients.add(this)
        Logger.isSillyEnabled && logger.silly(`Consumer::connect() - metadata:  ${JSON.stringify(metadata)}`)
      })
    })
  }

  /**
   * Returns the current connection status of the consumer
   *
   * @returns boolean
   */
  isConnected () {
    Logger.isSillyEnabled && this._config?.logger?.silly('Consumer::isConnected()')
    return this._consumer.isConnected()
  }

  /**
   * Returns the current connection time of the consumer
   *
   * @returns number
   */
  connectedTime () {
    Logger.isSillyEnabled && this._config?.logger?.silly('Consumer::connectedTime()')
    return this._consumer.connectedTime()
  }

  /**
   * Disconnect consumer
   *
   * Disconnects consumer from the Kafka broker
   */
  disconnect (cb = () => {}) {
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Consumer::disconnect() - start')
    if (this._pollInterval) {
      clearInterval(this._pollInterval)
    }
    this._status.running = false
    this._consumer.disconnect(cb)
    Logger.isSillyEnabled && logger.silly('Consumer::disconnect() - end')
  }

  /**
   * Subscribe
   *
   * Subscribes the consumer to the specified topics. If topics is null, then no action will be taken.
   * @param {object} topics - List of topics. Defaults: null
   */
  subscribe (topics = null) {
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Consumer::subscribe() - start')
    if (topics) {
      this._topics = topics
    }

    if (this._topics) {
      Logger.isSillyEnabled && logger.silly(`Consumer::subscribe() - subscribing too [${this._topics}]`)
      this._consumer.subscribe(this._topics)
    }
    Logger.isSillyEnabled && logger.silly('Consumer::subscribe() - end')
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
    Logger.isSillyEnabled && logger.silly('Consumer::consume() - start')

    if (!workDoneCb || typeof workDoneCb !== 'function') {
      workDoneCb = async () => {}
    }

    // setup queues to ensure sync processing of messages if options.sync is true
    if (this._config.options.sync) {
      this._syncQueue = async.queue((task, callbackDone) => {
        Logger.isSillyEnabled && logger.silly(`Consumer::consume()::syncQueue.queue[${this._syncQueue?.length()}] - Sync Process - ${JSON.stringify(task)}`)
        const payload = this._config.options.mode === ENUMS.CONSUMER_MODES.flow
          ? task.message
          : task.messages
        // todo: think how to start tracing span if payload has more than 1 message? (each message in the batch should have its own span?)

        const { span, newCtx } = this.#startTracingSpan(payload)

        context.with(newCtx, () => Promise.resolve(workDoneCb(task.error, payload))
          .then((result) => {
            callbackDone(task.error, result) // this marks the completion of the processing by the worker
            span.setStatus({ code: SpanStatusCode.OK })
          })
          .catch((err) => {
            Logger.isErrorEnabled && logger.error(`Consumer::consume()::syncQueue.queue[${this._syncQueue?.length()}] - workDoneCb - error: ${err}`)
            super.emit('error', err)
            callbackDone(err)
            span.setStatus({ code: SpanStatusCode.ERROR })
            span.recordException(err)
          })
          .finally(() => { span?.end() })
        )
      }, this._config.options.syncConcurrency)

      // a callback function, invoked when queue is empty.
      this._syncQueue.drain(() => {
        this._consumer.resume(this._topics) // resume listening new messages from the Kafka consumer group
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
            Logger.isSillyEnabled && logger.silly('Consumer::consume() - onRecursive - start')
            if (error) {
              Logger.isErrorEnabled && logger.error(`Consumer::consume() - onRecursive - error ${error}`)
            }
            if (this._status.running) {
              this._consumeRecursive(this._config.options.recursiveTimeout, this._config.options.batchSize, workDoneCb)
            } else {
              Logger.isDebugEnabled && logger.debug(`Consumer::consume() - onRecursive - status.running=${this._status.running}`)
            }
            Logger.isSillyEnabled && logger.silly('Consumer::consume() - onRecursive - end')
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
    Logger.isSillyEnabled && logger.silly('Consumer::consume() - end')
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
        if (error || !messages.length) {
          if (error) {
            super.emit('error', error)
            Logger.isErrorEnabled && logger.error(`Consumer::_consumerPoller() - ERROR - ${error}`)
          } else {
            Logger.isSillyEnabled && logger.silly('Consumer::_consumerPoller() - POLL EMPTY PING')
          }
        } else {
          // lets transform the messages into the desired format
          messages.forEach(msg => {
            const parsedValue = this._config.options.deserializeFn(msg.value, this._config.options)
            msg.value = parsedValue
            super.emit('message', msg)
          })

          if (Logger.isSillyEnabled) {
            if (this._config.options.messageAsJSON) {
              Logger.isDebugEnabled && logger.debug(`Consumer::_consumePoller() - messages[${messages.length}]: ${JSON.stringify(messages)}}`)
            } else {
              Logger.isDebugEnabled && logger.debug(`Consumer::_consumePoller() - messages[${messages.length}]: ${messages}}`)
            }
          }

          if (this._config.options.sync) {
            this._syncQueue.push({ error, messages }, function (err) {
              if (err) {
                Logger.isErrorEnabled && logger.error(`Consumer::_consumePoller()::syncQueue.push - error: ${error}`)
              }
            })
          } else {
            // todo: think how to start tracing span here (each message in the batch should have its own span?)
            Promise.resolve(workDoneCb(error, messages))
              .then((response) => {
                Logger.isDebugEnabled && logger.debug(`Consumer::_consumePoller() - non-sync wokDoneCb response - ${response}`)
              })
              .catch((err) => {
                Logger.isErrorEnabled && logger.error(`Consumer::_consumePoller() - non-sync wokDoneCb response - ${err}`)
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

        if (Logger.isSillyEnabled) {
          if (this._config.options.messageAsJSON) {
            logger.silly(`Consumer::_consumerRecursive() - messages[${messages.length}]: ${JSON.stringify(messages)}}`)
          } else {
            logger.silly(`Consumer::_consumerRecursive() - messages[${messages.length}]: ${messages}}`)
          }
        }

        if (this._config.options.sync) {
          // lets process the messages in batches
          if (!this._config.options.syncSingleMessage) {
            this._syncQueue.push({ error, messages }, (error, result) => {
              if (error) {
                Logger.isErrorEnabled && logger.error(`Consumer::_consumerRecursive()::syncQueue.Batch.push - error: ${error}`)
              }
              Logger.isDebugEnabled && logger.debug(`Consumer::_consumerRecursive()::syncQueue.Batch.push - result: ${result}`)
              super.emit('recursive', error, messages)
            })
          } else {
            // lets process each message individually
            for (const [index, msg] of messages.entries()) {
              this._syncQueue.push({ error, messages: msg }, (error, result) => {
                if (error) {
                  Logger.isErrorEnabled && logger.error(`Consumer::_consumerRecursive()::syncQueue.Single.push - error: ${error}`)
                }
                Logger.isDebugEnabled && logger.debug(`Consumer::_consumerRecursive()::syncQueue.Single.push - result: ${result}`)
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
              Logger.isDebugEnabled && logger.debug(`Consumer::_consumerRecursive() - non-sync wokDoneCb response - ${response}`)
              super.emit('recursive', error, messages)
            }).catch((err) => {
              Logger.isErrorEnabled && logger.error(`Consumer::_consumerRecursive() - non-sync wokDoneCb response - ${err}`)
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
      if (error || !message) {
        if (error) {
          super.emit('error', error)
        }
      } else {
        const parsedValue = this._config.options.deserializeFn(message.value, this._config.options)
        message.value = parsedValue
        super.emit('message', message)

        if (Logger.isSillyEnabled) {
          if (this._config.options.messageAsJSON) {
            Logger.isDebugEnabled && logger.debug(`Consumer::_consumerFlow() - message: ${JSON.stringify(message)}`)
          } else {
            Logger.isDebugEnabled && logger.debug(`Consumer::_consumerFlow() - message: ${message}`)
          }
        }

        if (this._config.options.sync) {
          this._syncQueue.push({ error, message }, function (err) {
            if (err) { Logger.isErrorEnabled && logger.error(err) }
          })
        } else {
          // todo: think how to start tracing span here (each message in the batch should have its own span?)
          Promise.resolve(workDoneCb(error, message))
            .then((response) => {
              Logger.isDebugEnabled && logger.debug(`Consumer::_consumerFlow() - non-sync wokDoneCb response - ${response}`)
            }).catch((err) => {
              Logger.isErrorEnabled && logger.error(`Consumer::_consumerFlow() - non-sync wokDoneCb response - ${err}`)
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
    Logger.isSillyEnabled && logger.silly('Consumer::commit() - start')
    this._consumer.commit(topicPartitions)
    Logger.isSillyEnabled && logger.silly('Consumer::commit() - end')
  }

  /**
   * Commit message
   *
   * @param {KafkaConsumer~Message} msg - Kafka message to be commited
   */
  commitMessage (msg) {
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Consumer::commitMessage() - start')
    this._consumer.commitMessage(msg)
    Logger.isSillyEnabled && logger.silly('Consumer::commitMessage() - end')
  }

  /**
   * Commit topics partition in sync mode
   *
   * @param {object} topicPartitions - List of topics that must be commited. If null, it will default to the topics list provided in the constructor. Defaults = null
   */
  commitSync (topicPartitions = null) {
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Consumer::commitSync() - start')
    this._consumer.commitSync(topicPartitions)
    Logger.isSillyEnabled && logger.silly('Consumer::commitSync() - end')
  }

  /**
   * Commit message in sync mode
   *
   * @param {KafkaConsumer~Message} msg - Kafka message to be commited
   */
  commitMessageSync (msg) {
    const { logger } = this._config
    Logger.isSillyEnabled && logger.silly('Consumer::commitMessageSync() - start')
    this._consumer.commitMessageSync(msg)
    Logger.isSillyEnabled && logger.silly('Consumer::commitMessageSync() - end')
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
    Logger.isSillyEnabled && logger.silly('Consumer::getWatermarkOffsets() - start')
    Logger.isSillyEnabled && logger.silly('Consumer::getWatermarkOffsets() - end')
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
    Logger.isSillyEnabled && logger.silly('Consumer::getMetadata() - start')
    this._consumer.getMetadata(metadataOptions, metaDatacCb)
    Logger.isSillyEnabled && logger.silly('Consumer::getMetadata() - end')
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
      Logger.isSillyEnabled && logger.silly('Consumer::getMetadataSync() - start')
      this._consumer.getMetadata(metadataOptions, metaDatacCb)
      Logger.isSillyEnabled && logger.silly('Consumer::getMetadataSync() - end')
    })
  }

  static _parseBuffer (buffer, encoding, asJson) {
    return Protocol.parseValue(buffer, encoding, asJson)
  }

  #startTracingSpan (payload) {
    // think, how to start tracing span if payload has more than 1 message? (each message in the batch should have its own span?)
    const { headers, topic } = Array.isArray(payload)
      ? payload[0]
      : payload

    const otelHeaders = (!Array.isArray(headers) || headers.length === 0)
      ? {}
      : headers.reduce((acc, header) => {
        Object.entries(header).forEach(([key, value]) => {
          if (OTEL_HEADERS.includes(key)) acc[key] = value?.toString()
        })
        return acc
      }, {})

    const activeContext = Object.keys(otelHeaders).length
      ? propagation.extract(context.active(), otelHeaders)
      : context.active()

    const span = tracer.startSpan(`RECEIVE:${topic}`, { kind: SpanKind.CONSUMER }, activeContext)
    const newCtx = trace.setSpan(activeContext, span)

  span.setAttributes({
    [SemConv.ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: this._config.options.batchSize,
    [SemConv.ATTR_MESSAGING_CLIENT_ID]: this._config.rdkafkaConf['client.id'],
    [SemConv.ATTR_MESSAGING_CONSUMER_GROUP_NAME]: this._config.rdkafkaConf['group.id'],
    [SemConv.ATTR_MESSAGING_DESTINATION_NAME]: topic,
    [SemConv.ATTR_MESSAGING_OPERATION_NAME]: 'receive',
    [SemConv.ATTR_MESSAGING_SYSTEM]: 'kafka',
    [SemConv.ATTR_SERVER_ADDRESS]: this._config.rdkafkaConf['metadata.broker.list']
    // think, if we need more attributes
  })

    return { span, newCtx }
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
