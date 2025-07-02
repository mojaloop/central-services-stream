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

 * Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 * Miguel de Barros <miguel.debarros@modusbox.com>
 --------------
 ******/
'use strict'

/**
 * @module src/handlers/lib/kafka
 */

const Producer = require('../../src').Kafka.Producer
const Logger = require('@mojaloop/central-services-logger')
const ErrorHandler = require('@mojaloop/central-services-error-handling')
const { stateList } = require('../constants')

const listOfProducers = {}

/**
 * @function ProduceMessage
 *
 * @param {object} messageProtocol - message being created against topic
 * @param {object} topicConf - configuration for the topic to produce to
 * @param {object} config - Producer configuration, eg: to produce batch or poll
 *
 * @description Creates a producer on Kafka for the specified topic and configuration
 *
 * @returns {boolean} - returns true if producer successfully created and producers to
 * @throws {error} - if not successfully create/produced to
 */
const produceMessage = async (messageProtocol, topicConf, config) => {
  try {
    let producer
    if (listOfProducers[topicConf.topicName]) {
      producer = listOfProducers[topicConf.topicName]
    } else {
      Logger.isDebugEnabled && Logger.debug('Producer::start::topic=' + topicConf.topicName)
      producer = new Producer(config)
      Logger.isDebugEnabled && Logger.debug('Producer::connect::start')
      await producer.connect()
      Logger.isDebugEnabled && Logger.debug('Producer::connect::end')
      listOfProducers[topicConf.topicName] = producer
    }
    Logger.isDebugEnabled && Logger.debug(`Producer.sendMessage::messageProtocol:'${JSON.stringify(messageProtocol)}'`)
    await producer.sendMessage(messageProtocol, topicConf)
    Logger.isDebugEnabled && Logger.debug('Producer::end')
    return true
  } catch (err) {
    Logger.isErrorEnabled && Logger.error(err)
    Logger.isDebugEnabled && Logger.debug(`Producer error has occurred for ${topicConf.topicName}`)
    throw ErrorHandler.Factory.reformatFSPIOPError(err)
  }
}

/**
 * @function connectAll
 *
 * @param {array} configs - and array of topic and kafka configs
 *
 * @description Connects all Producers for the passed in topic configurations
 *
 * @returns null
 */
const connectAll = async (configs) => {
  for (const config of configs) {
    try {
      let producer
      if (!listOfProducers[config.topicConfig.topicName]) {
        Logger.isDebugEnabled && Logger.debug('Producer::start::topic=' + config.topicConfig.topicName)
        producer = new Producer(config.kafkaConfig)
        Logger.isDebugEnabled && Logger.debug('Producer::connect::start')
        await producer.connect()
        Logger.isDebugEnabled && Logger.debug('Producer::connect::end')
        listOfProducers[config.topicConfig.topicName] = producer
      }
    } catch (err) {
      Logger.isErrorEnabled && Logger.error(err)
      Logger.isDebugEnabled && Logger.debug(`Producer error has occurred for ${config.topicConf.topicName}`)
    }
  }
}

const disconnectAndRemoveProducer = async (topicName) => {
  await getProducer(topicName).disconnect()
  delete listOfProducers[topicName]
}

/**
 * @function Disconnect
 *
 * @param {string} topicName - Producer of the specified topic to be disconnected. If this is null, then ALL producers will be disconnected. Defaults: null.
 *
 * @description Disconnects a specific producer, or ALL producers from Kafka
 *
 * @returns {object} Promise
 */
const disconnect = async (topicName = null) => {
  if (topicName && typeof topicName === 'string') {
    try {
      await disconnectAndRemoveProducer(topicName)
    } catch (err) {
      Logger.isErrorEnabled && Logger.error(err)
      throw ErrorHandler.Factory.reformatFSPIOPError(err)
    }
  } else if (topicName === null) {
    let isError = false
    const errorTopicList = []
    let tpName
    for (tpName in listOfProducers) {
      try {
        await disconnectAndRemoveProducer(tpName)
      } catch (e) {
        isError = true
        errorTopicList.push({ topic: tpName, error: e.toString() })
      }
    }
    if (isError) {
      throw ErrorHandler.Factory.createInternalServerFSPIOPError(`The following Producers could not be disconnected: ${JSON.stringify(errorTopicList)}`)
    }
  } else {
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Unable to disconnect Producer: ${topicName}`)
  }
}

/**
 * @function GetProducer
 *
 * @param {string} topicName - the topic name to locate a specific producer
 *
 * @description This is used to get a producer with the topic name to send messages to a kafka topic
 *
 * @returns {Producer} - Returns producer
 * @throws {Error} - if producer not found for topic name
 */
const getProducer = (topicName) => {
  if (listOfProducers[topicName]) {
    return listOfProducers[topicName]
  } else {
    throw ErrorHandler.Factory.createInternalServerFSPIOPError(`No producer found for topic ${topicName}`)
    // clarify, why we throw an error here and not just return null?
  }
}

/**
 * @function isConnected
 *
 * @param {string} topicName - the topic name of the producer to check
 *
 * @description
 * Checks if the producer is connected to the broker. Note: Due to the underlying implementation of node-rdkafka,
 * the `isConnected()` method only returns false if the producer is manually disconnected.
 * For more robust checks (e.g., topic existence or partition assignment), use
 * `getMetadataPromise` or `allConnected`.
 *
 * https://github.com/Blizzard/node-rdkafka/issues/217#issuecomment-313582908
 *
 * @returns boolean - if connected
 * @throws {Error} - if producer can't be found or the producer is not connected
 */
const isConnected = async (topicName = undefined) => {
  if (!topicName) {
    Logger.isDebugEnabled && Logger.debug('topicName is undefined.')
    throw ErrorHandler.Factory.createInternalServerFSPIOPError('topicName is undefined.')
  }
  const producer = getProducer(topicName)
  return producer.isConnected()
}

/**
 * @function getMetadataPromise
 *
 * @param {object} producer - the producer class
 * @param {string} topic - the topic name of the producer to check
 *
 * @description Use this to determine whether or not we are connected to the broker. Internally, it calls `getMetadata` to determine
 * if the broker client is connected.
 *
 * @returns object - resolve metadata object
 * @throws {Error} - if Producer can't be found or the producer is not connected
 */
const getMetadataPromise = async (producer, topic) => {
  return new Promise((resolve, reject) => {
    const cb = async (err, metadata) => {
      if (err) {
        return reject(new Error(`Error connecting to producer: ${err.message}`))
      }
      return resolve(metadata)
    }
    producer.getMetadata({ topic, timeout: 6000 }, cb)
  })
}

const allConnected = async () => {
  for (const [key, producer] of Object.entries(listOfProducers)) {
    // Use isEventStatsConnectionHealthy if available, otherwise fallback to metadata check
    if (typeof producer.isEventStatsConnectionHealthy === 'function') {
      const healthy = producer.isEventStatsConnectionHealthy()
      if (!healthy) {
        Logger.isDebugEnabled && Logger.debug(`Producer connection for topic ${key} is not healthy.`)
        throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Producer connection for topic ${key} is not healthy.`)
      }
    } else {
      // Fallback to metadata check
      const metadata = await getMetadataPromise(producer, key)
      const foundTopics = metadata.topics.map(topic => topic.name)
      if (!foundTopics.includes(key)) {
        Logger.isDebugEnabled && Logger.debug(`Connected to producer, but ${key} not found in metadata.`)
        throw ErrorHandler.Factory.createInternalServerFSPIOPError(`Connected to producer, but ${key} not found in metadata.`)
      }
    }
  }
  return stateList.OK
}

module.exports = {
  getProducer,
  produceMessage,
  disconnect,
  isConnected,
  allConnected,
  connectAll
}
