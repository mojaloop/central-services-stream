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
'use strict'

/**
 * @module src/handlers/lib/kafka
 */

const Consumer = require('../../src').Kafka.Consumer
const ErrorHandler = require('@mojaloop/central-services-error-handling')
const { stateList } = require('../constants')
const logger = require('../lib/logger').logger

const listOfConsumers = {}

/**
 * Health tracking for consumers.
 * Each topicName maps to an object: { healthy: boolean, timer: Timeout|null }
 */
const consumerHealth = {}

// Default health timer duration in milliseconds
let consumerHealthTimerMs = 10000

/**
 * Get the current consumer health timer duration in ms.
 */
function getConsumerHealthTimerMs () {
  return consumerHealthTimerMs
}

/**
 * Set the consumer health timer duration in ms.
 * @param {number} ms
 */
function setConsumerHealthTimerMs (ms) {
  consumerHealthTimerMs = ms
}

/**
 * Wrapper for consumer.consume to track health.
 * If an error occurs, sets a timer for consumerHealthTimerMs to mark unhealthy.
 * Any successful consume clears the timer and marks healthy.
 */
function consumeWithHealthTracking (consumer, topicName, command) {
  // Wrap the original command to track health
  const wrappedCommand = async (error, messages) => {
    if (error) {
      // Start a timer if not already started
      if (!consumerHealth[topicName]) consumerHealth[topicName] = { healthy: true, timer: null }
      if (!consumerHealth[topicName].timer) {
        consumerHealth[topicName].timer = setTimeout(() => {
          consumerHealth[topicName].healthy = false
          logger.warn(`Consumer health timer expired for topic ${topicName}, marking as unhealthy`)
        }, consumerHealthTimerMs)
      }
    } else {
      // On success, clear timer and mark healthy
      if (!consumerHealth[topicName]) consumerHealth[topicName] = { healthy: true, timer: null }
      if (consumerHealth[topicName].timer) {
        clearTimeout(consumerHealth[topicName].timer)
        consumerHealth[topicName].timer = null
      }
      consumerHealth[topicName].healthy = true
    }
    // Call the original command
    return command(error, messages)
  }
  consumer.consume(wrappedCommand)
}

/**
 * @function CreateHandler
 *
 * @param {string | string[]} topicName - the topic name to be registered for the required handler. Example: 'topic-dfsp1-transfer-prepare'
 * @param {object} config - the config for the consumer for the specific functionality and action, retrieved from the default.json. Example: found in default.json 'KAFKA.CONSUMER.TRANSFER.PREPARE'
 * @param {function} command - the callback handler for the topic. Will be called when the topic is produced against. Example: Command.prepareHandler()
 *
 * @description Parses the accountUri into a participant name from the uri string
 *
 * @returns {object} - Returns a Promise
 * @throws {Error} -  if failure occurs
 */
const createHandler = async (topicName, config, command) => {
  logger.debug(`CreateHandler::connect - creating Consumer for topics: [${topicName}]`)
  const topicNameArray = Array.isArray(topicName)
    ? topicName
    : [topicName]
  const consumer = new Consumer(topicNameArray, config)

  let autoCommitEnabled = true
  // istanbul ignore next
  if (config.rdkafkaConf !== undefined && config.rdkafkaConf['enable.auto.commit'] !== undefined) {
    autoCommitEnabled = config.rdkafkaConf['enable.auto.commit']
  }
  if (config.rdkafkaConf !== undefined && config.rdkafkaConf.enableAutoCommit !== undefined) {
    autoCommitEnabled = config.rdkafkaConf.enableAutoCommit
  }

  let connectedTimeStamp = 0
  try {
    await consumer.connect()
    logger.verbose(`CreateHandler::connect - successfully connected to topics: [${topicNameArray}]`)
    connectedTimeStamp = (new Date()).valueOf()
    // Use the health-tracking wrapper
    topicNameArray.forEach(topic => {
      consumerHealth[topic] = { healthy: true, timer: null }
    })
    consumeWithHealthTracking(consumer, topicNameArray[0], command)
  } catch (e) {
    // Don't throw the error, still keep track of the topic we tried to connect to
    logger.warn(`CreateHandler::connect - error: ${e}`)
  }

  topicNameArray.forEach(topicName => {
    listOfConsumers[topicName] = {
      consumer,
      autoCommitEnabled,
      connectedTimeStamp
    }
  })
}

/**
 * @function GetConsumer
 *
 * @param {string} topicName - the topic name to locate a specific consumer
 *
 * @description This is used to get a consumer with the topic name to commit the messages that have been received
 *
 * @returns {Consumer} - Returns consumer
 * @throws {Error} - if consumer not found for topic name
 */
const getConsumer = (topicName) => {
  if (listOfConsumers[topicName]) {
    return listOfConsumers[topicName].consumer
  } else {
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`No consumer found for topic ${topicName}`)
  }
}

/**
 * @function isConsumerAutoCommitEnabled
 *
 * @param {string} topicName - the topic name to locate a specific consumer
 *
 * @description This is used to get a consumer with the topic name to commit the messages that have been received
 *
 * @returns {Consumer} - Returns consumer
 * @throws {Error} - if consumer not found for topic name
 */
const isConsumerAutoCommitEnabled = (topicName) => {
  if (listOfConsumers[topicName]) {
    return listOfConsumers[topicName].autoCommitEnabled
  } else {
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`No consumer found for topic ${topicName}`)
  }
}

/**
 * @function getListOfTopics
 *
 *
 * @description Get a list of topics that the consumer has subscribed to
 *
 * @returns {Array<string>} - list of topics
 */
const getListOfTopics = () => {
  return Object.keys(listOfConsumers)
}

/**
 * @function isConnected
 *
 * @param {string} topicName - the topic name of the consumer to check
 *
 * @description
 * Checks if the consumer is connected to the broker. Note: Due to the underlying implementation of node-rdkafka,
 * the `isConnected()` method only returns false if the consumer is manually disconnected.
 * For more robust checks (e.g., topic existence or partition assignment), use
 * `getMetadataPromise` or `allConnected`.
 *
 * https://github.com/Blizzard/node-rdkafka/issues/217#issuecomment-313582908
 *
 * @returns {boolean} - true if the consumer is connected to at least one broker, false otherwise
 * @throws {Error} - if consumer can't be found for the topic name or topicName is undefined
 */
const isConnected = async (topicName = undefined) => {
  if (!topicName) {
    logger.debug('topicName is undefined.')
    throw ErrorHandler.Factory.createInternalServerFSPIOPError('topicName is undefined.')
  }
  const consumer = getConsumer(topicName)
  return consumer.isConnected()
}

/**
 * @function getMetadataPromise
 *
 * @param {object} consumer - the consumer class
 * @param {string} topic - the topic name of the consumer to check
 *
 * @description Use this to determine whether or not we are connected to the broker. Internally, it calls `getMetadata` to determine
 * if the broker client is connected.
 *
 * @returns object - resolve metadata object
 * @throws {Error} - if consumer can't be found or the consumer is not connected
 */
const getMetadataPromise = (consumer, topic) => {
  return new Promise((resolve, reject) => {
    const cb = (err, metadata) => {
      if (err) {
        return reject(new Error(`Error connecting to consumer: ${err.message}`))
      }

      return resolve(metadata)
    }
    consumer.getMetadata({ topic, timeout: 6000 }, cb)
  })
}

/**
 * @function allConnected
 *
 * @param {string} topicName - the topic name of the consumer to check
 *
 * @description Use this to determine whether or not we are connected to the broker. Internally, it calls `getMetadata` to determine
 * if the broker client is connected.
 *
 * @returns boolean - if connected
 * @throws {Error} - if consumer can't be found or the consumer is not connected
 */
const allConnected = async topicName => {
  // Use the health variable
  if (consumerHealth[topicName] && consumerHealth[topicName].healthy === false) {
    logger.error(`Consumer health variable indicates unhealthy connection for topic ${topicName}`)
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Consumer health variable indicates unhealthy connection for topic ${topicName}`)
  }

  const consumer = getConsumer(topicName)

  // Use the isEventStatsConnectionHealthy method from the consumer
  if (typeof consumer.isEventStatsConnectionHealthy === 'function') {
    if (!consumer.isEventStatsConnectionHealthy()) {
      logger.error(`Consumer event.stats indicates unhealthy connection for topic ${topicName}`)
      throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Consumer event.stats indicates unhealthy connection for topic ${topicName}`)
    }
  }

  const metadata = await getMetadataPromise(consumer, topicName)
  const foundTopics = metadata.topics.map(topic => topic.name)
  if (!foundTopics.includes(topicName)) {
    logger.error(`Connected to consumer, but ${topicName} not found.`)
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Connected to consumer, but ${topicName} not found.`)
  }
  return stateList.OK
}

module.exports = {
  Consumer,
  createHandler,
  getConsumer,
  getListOfTopics,
  isConsumerAutoCommitEnabled,
  isConnected,
  getMetadataPromise,
  allConnected,
  getConsumerHealthTimerMs,
  setConsumerHealthTimerMs
}
