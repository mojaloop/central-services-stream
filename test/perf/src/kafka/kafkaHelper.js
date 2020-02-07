'use strict'

const Consumer = require('@mojaloop/central-services-stream').Kafka.Consumer
const Producer = require('@mojaloop/central-services-stream').Kafka.Producer
// const ConsumerEnums = require('@mojaloop/central-services-shared').Kafka.Consumer.ENUMS
const Logger = require('@mojaloop/central-services-logger')

const createConsumer = async (topicList, consumeFunction, config) => {
  Logger.debug('createConsumer::start')
  Logger.log('createConsumer::- Instantiate consumer')

  // set the logger
  config.logger = Logger

  var c = new Consumer(topicList, config)

  Logger.info('createConsumer::- Connecting...')
  var connectionResult = await c.connect()

  Logger.info(`createConsumer::- Connected result=${connectionResult}`)

  Logger.debug('createConsumer::- Consume messages')

  c.consume(consumeFunction)

  // // consume 'ready' event
  // c.on('ready', arg => console.log(`onReady: ${JSON.stringify(arg)}`))
  // // consume 'message' event
  // c.on('message', message => console.log(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`))
  // // consume 'batch' event
  // c.on('batch', message => console.log(`onBatch: ${JSON.stringify(message)}`))

  Logger.debug(`createConsumer::end - returns ${c}`)
  return c
}

const createProducer = async (config) => {
  Logger.debug('createProducer::start')

  // set the logger
  config.logger = Logger

  var p = new Producer(config)

  Logger.info('createProducer::- Connecting...')
  var connectionResult = await p.connect()
  Logger.info(`createProducer::- Connected result=${connectionResult}`)

  Logger.debug('createProducer::end')
  return p
}

exports.createConsumer = createConsumer
exports.createProducer = createProducer
