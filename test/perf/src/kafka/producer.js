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

 * ModusBox
 Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/

'use strict'

const KafkaHelper = require('./kafkaHelper')
const Mustache = require('mustache')
const uuidv4 = require('uuid/v4')
const Config = require('@local/config')
const Logger = require('@mojaloop/central-services-logger')
const _ = require('lodash')
const Faker = require('faker')

const runProducer = async (messageNum = 1, messageSize = 10, topicName) => {
const batchId = uuidv4()
const batchStart = (new Date()).getTime()
Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}] ~ Producer::perf::runProducer - START`)
const config = Config.PRODUCER.KAFKA.configs

var topicConf = {
    topicName: topicName || Config.PRODUCER.KAFKA.topics[0]
}

var producerClient = await KafkaHelper.createProducer(config)

if(messageNum > 1){
    Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}] ~ Producer::perf::runProducer -  Sending ${messageNum} messages for batch '${batchId}'`)
} else {
    Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}] ~ Producer::perf::runProducer - Sending a single messages`)
}

const randomPayload = Faker.random.alphaNumeric(messageSize)

for(var i = 0; i <  messageNum; i++){
    var messageValues = {
    id: uuidv4(),
    batchId: batchId,
    batchStart: batchStart,
    start: (new Date()).getTime(),
    payload: randomPayload
    }
    var message
    var result
    try {
        message= JSON.parse(Mustache.render(Config.TEMPLATES.messages[0], messageValues))
        Logger.debug(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}, tid=${messageValues.id}] ~ Producer::perf::runProducer - Sending payload [${i+1}] - ${JSON.stringify(message)}`)
        result = await producerClient.sendMessage(message, topicConf)
        Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}, tid=${messageValues.id}] ~ Producer::perf::runProducer - Message[${i+1}] sent with result: ${result}`)
    } catch (err) {
        Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}, tid=${messageValues.id}] ~ Producer::perf::runProducer - Message[${i+1}] sent with error: ${result}`)
        Logger.error(err)
    }
}

    Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}] ~ Producer::perf::runProducer - Producer for '${topicName}' Disconnecting`)
    producerClient.disconnect()
    Logger.info(`[cid=${batchId}, messageNum=${messageNum}, messageSize=${messageSize}, topicName=${topicName}] ~ Producer::perf::runProducer - END`)
}
// Logger.debug(`process.argv=${process.argv}`)
// if(process.argv.length == 3 && !isNaN(process.argv[2])){
//   Logger.debug(`2 = ${process.argv[2]}`)
//   runProducer(parseInt(process.argv[2]))
// } else {
//   Logger.debug(`0`)
//   runProducer()
// }

module.exports = {
    run: runProducer
}
