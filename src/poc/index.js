
'use strict'

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

 * Miguel de Barros <miguel.debarros@modusbox.com> <-- Adapted into OSS Stream

 --------------
 ******/

const Logger = require('@mojaloop/central-services-shared').Logger
const ConsumerFlow = require('./consumerFlow')
const ConsumerFlowSync = require('./consumerFlowSync')
const ConsumerBatch = require('./consumerBatch')
const StreamConsumer = require('./streamConsumer')
const Node = require('./node')

const KafkaConsumer = require('../kafka').Consumer

const ENUMS = {
  flow: 0,
  poll: 1,
  recursive: 2,
  pocConsumerFlow: 3,
  pocConsumerBatch: 4,
  pocStream: 5,
  pocNode: 6,
  pocConsumerFlowSync: 7
}

const createConsumer = async (topicName = [], config = {}, command) => {
  let consumer = {}
  let topicList = []

  if (Array.isArray(topicName)) {
    topicList = topicName
  } else {
    topicList = [topicName]
  }

  switch (config.options.mode) {
    case ENUMS.flow:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka Flow Consumer`)
      consumer = new KafkaConsumer(topicList, config)
      await consumer.connect().then(async () => {
        Logger.info(`CreateHandle::connect successful topic: ${topicName}`)
        await consumer.consume(command)
      }).catch((e) => {
        Logger.error(e)
        Logger.info('Consumer error has occurred')
        throw e
      })
      break
    case ENUMS.poll:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka Poll Consumer`)
      consumer = new KafkaConsumer(topicList, config)
      await consumer.connect().then(async () => {
        Logger.info(`CreateHandle::connect successful topic: ${topicName}`)
        await consumer.consume(command)
      }).catch((e) => {
        Logger.error(e)
        Logger.info('Consumer error has occurred')
        throw e
      })
      break
    case ENUMS.recursive:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka Recursive Consumer`)
      consumer = new KafkaConsumer(topicList, config)
      await consumer.connect().then(async () => {
        Logger.info(`CreateHandle::connect successful topic: ${topicName}`)
        await consumer.consume(command)
      }).catch((e) => {
        Logger.error(e)
        Logger.info('Consumer error has occurred')
        throw e
      })
      break
    case ENUMS.pocConsumerFlow:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC Flow Consumer`)
      consumer = new ConsumerFlow(topicList, config, command)
      consumer.connect()
      consumer.on('ready', () => {
        Logger.info(`createConsumer['${topicName}'] - connected`)
      })
      break
    case ENUMS.pocConsumerFlowSync:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC FlowSync Consumer`)
      consumer = new ConsumerFlowSync(topicList, config, command)
      consumer.connect()
      consumer.on('ready', () => {
        Logger.info(`createConsumer['${topicName}'] - connected`)
      })
      break
    case ENUMS.pocConsumerBatch:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC Batch Consumer`)
      consumer = new ConsumerBatch(topicList, config, command)
      consumer.connect()
      consumer.on('ready', () => {
        Logger.info(`createConsumer['${topicName}'] - connected`)
      })
      break
    case ENUMS.pocStream:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC Stream Consumer`)
      consumer = new StreamConsumer(topicList, config, command)
      Logger.info(`createConsumer['${topicName}'] - connected`)
      break
    case ENUMS.pocNode:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC Node Consumer`)
      consumer = new Node(topicList, config, command)
      break
    default:
      Logger.info(`createConsumer['${topicName}'] - creating Kafka PoC Stream Consumer - DEFAULT`)
      consumer = new StreamConsumer(topicList, config, command)
      Logger.info(`createConsumer['${topicName}'] - connected`)
  }
  return consumer
}

module.exports = {
  ConsumerFlow: require('./consumerFlow'),
  ConsumerBatch: require('./consumerBatch'),
  StreamConsumer: require('./streamConsumer'),
  Node: require('./node'),
  createConsumer: createConsumer,
  ENUMS: ENUMS
}
