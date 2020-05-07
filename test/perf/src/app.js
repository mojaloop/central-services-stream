
const PJson = require('../package.json')
const Logger = require('@mojaloop/central-services-logger')
const { Command } = require('commander')
const KafkaProducer = require('./kafka/producer')
const KafkaConsumer = require('./kafka/consumer')
const Setup = require('./shared/setup').init
const Config = require('@local/config')

const main = async () => {


}