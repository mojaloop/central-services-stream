const Consumer = require('@mojaloop/central-services-stream').Kafka.Consumer
const ConsumerEnums = require('@mojaloop/central-services-stream').Kafka.Consumer.ENUMS
const Logger = require('@mojaloop/central-services-logger')
const Sampler = require('#utils/sampler')

class Test extends Sampler {
  constructor (opts) {
    super(opts)

    this.consumerConf = opts?.consumerConf || {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        pollFrequency: 10,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        consumeTimeout: 1000
      },
      rdkafkaConf: {
        'client.id': 'cl-test',
        'group.id': 'cl-group-test',
        'metadata.broker.list': 'localhost:9092',
        'socket.keepalive.enable': true,
        'allow.auto.create.topics': true,
        'enable.partition.eof': true
      },
      topicConf: {
        'auto.offset.reset': 'earliest'
      },
      logger: Logger
    }

    this.topicList = opts?.topicList || [
      'test'
    ]
  }

  async beforeAll () {
    console.log(`test:${this.opts.name}::beforeAll`)

    this.client = new Consumer(this.topicList, this.consumerConf)

    const connectionResult = await this.client.connect()

    this.opts.debug && console.log(`Connected result=${connectionResult}`)

    // consume 'ready' event
    this.client.on('ready', arg => console.log(`onReady: ${JSON.stringify(arg)}`))
    // // consume 'message' event
    // this.client.on('message', message => console.log(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`))
    // // consume 'batch' event
    // this.client.on('batch', message => console.log(`onBatch: ${JSON.stringify(message)}`))
    super.beforeAll()
  }

  async run () {
    this.client.consume((error, message) => {
      return new Promise((resolve, reject) => {
        if (error) {
          console.error(error)
          // resolve(false)
          reject(error)
        }
        if (message) { // check if there is a valid message coming back
          this.stat.count++
          // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
          if (Array.isArray(message) && message.length != null && message.length > 0) {
            message.forEach(msg => {
              this.opts.debug && console.log(`Message received[${msg.value.id}] - offset=${msg.offset}`)
              this.client.commitMessage(msg)
            })
          } else {
            this.opts.debug && console.log(`Message received[${message.value.id}] - offset=${message.offset}`)
            this.client.commitMessage(message)
          }
          resolve(true)
        } else {
          resolve(false)
        }
      })
    })

    return new Promise(resolve => {
      this.client.on('partition.eof', eof => {
        this.opts.debug && console.log(`eof: ${JSON.stringify(eof)}`)
        this.client.disconnect()
        resolve(true)
      })
    })
  }

  async afterAll () {
    console.log(`test:${this.opts.name}::afterAll`)
    super.afterAll()
    await this.client.disconnect()
  }
}

module.exports = Test
