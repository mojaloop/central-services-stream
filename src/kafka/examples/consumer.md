*Example Consumer*

```JSON
const Consumer = require('@mojaloop/central-services-stream').Kafka.Consumer
const ConsumerEnums = require('@mojaloop/central-services-stream').Kafka.Consumer.ENUMS
const logger = require('../lib/logger').logger

const testConsumer = async () => {
  logger.info('Instantiate consumer')
  var c = new Consumer(['test1'], {
    options: {
      mode: ConsumerEnums.CONSUMER_MODES.recursive,
      batchSize: 1,
      recursiveTimeout: 100,
      messageCharset: 'utf8',
      messageAsJSON: true,
      sync: true,
      consumeTimeout: 1000
    },
    rdkafkaConf: {
      'group.id': 'kafka-test',
      'metadata.broker.list': 'localhost:9092',
      'enable.auto.commit': false
    },
    topicConf: {},
    logger
  })

  logger.debug('Connect consumer')
  var connectionResult = await c.connect()

  logger.debug(`Connected result=${connectionResult}`)

  logger.debug('Consume messages')

  c.consume((error, message) => {
    return new Promise((resolve, reject) => {
      if (error) {
        logger.debug(`WTDSDSD!!! error ${error}`)
        // resolve(false)
        reject(error)
      }
      if (message) { // check if there is a valid message coming back
        logger.debug(`Message Received by callback function - ${JSON.stringify(message)}`)
        // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
        if (Array.isArray(message) && message.length != null && message.length > 0) {
          message.forEach(msg => {
            c.commitMessage(msg)
          })
        } else {
          c.commitMessage(message)
        }
        resolve(true)
      } else {
        resolve(false)
      }
      // resolve(true)
    })
  })

  // consume 'ready' event
  c.on('ready', arg => logger.debug(`onReady: ${JSON.stringify(arg)}`))
  // consume 'message' event
  c.on('message', message => logger.debug(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`))
  // consume 'batch' event
  c.on('batch', message => logger.debug(`onBatch: ${JSON.stringify(message)}`))

  logger.debug('testConsumer::end')
}

testConsumer()
```
