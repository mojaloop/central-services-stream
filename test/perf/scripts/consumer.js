const { inspect } = require('util')
const Consumer = require('@mojaloop/central-services-stream').Kafka.Consumer
const ConsumerEnums = require('@mojaloop/central-services-stream').Kafka.Consumer.ENUMS
const Protocol = require('@mojaloop/central-services-stream').Kafka.Protocol
const Logger = require('@mojaloop/central-services-logger')

const Sampler = require('#utils/sampler')

class Test extends Sampler {
  constructor (opts) {
    super(opts)

    // Example deserializeFn override
    // eslint-disable-next-line no-unused-vars
    const overrideDeserializeFn = (buffer, opts) => {
      return Protocol.parseValue(buffer, opts.messageCharset, opts.messageAsJSON)
    }
    this.maxMessages = opts?.maxMessages || null
    this.consumerConf = opts?.consumerConf || {
      options: {
        mode: ConsumerEnums.CONSUMER_MODES.recursive,
        batchSize: 1,
        pollFrequency: 10,
        recursiveTimeout: 100,
        messageCharset: 'utf8',
        messageAsJSON: true,
        sync: true,
        syncConcurrency: 1,
        consumeTimeout: 1000,
        deserializeFn: null // Use this if you want to use default deserializeFn
        // deserializeFn: overrideDeserializeFn // Use this if you want to override the default deserializeFn
      },
      rdkafkaConf: {
        'client.id': 'test',
        'group.id': 'cl-group-test',
        'metadata.broker.list': 'localhost:9092',
        'statistics.interval.ms': 0, // Enable event.stats event if value is greater than 0
        // 'statistics.interval.ms': 100, // Enable event.stats event if value is greater than 0
        'socket.keepalive.enable': true,
        'allow.auto.create.topics': true,
        'partition.assignment.strategy': 'range,roundrobin', // range, roundrobin, cooperative-sticky. - Cooperative and non-cooperative (eager) strategies must not be mixed
        'enable.partition.eof': true
        // offset_commit_cb: (err, topicPartitions) => {
        //   if (err) {
        //     // There was an error committing
        //     console.error(err)
        //   }
        //   console.log(topicPartitions)
        // }
      },
      topicConf: {
        'auto.offset.reset': 'earliest'
      },
      logger: Logger
    }

    this.topicList = opts?.topicList || [
      'test'
    ]

    this.stat.labels = {
      sync: this.consumerConf.options.sync
    }
  }

  async beforeAll (opts) {
    console.log(`test:${this.opts.name}::beforeAll`)

    this.maxMessages = this?.maxMessages || opts?.maxMessages

    this.client = new Consumer(this.topicList, this.consumerConf)

    this.client.on('ready', args => console.log('ready:', args))

    const connectionResult = await this.client.connect()

    this.opts.debug && console.log(`Connected result=${connectionResult}`)

    // this.client.getMetadata({ topic: this.topicList[0], timeout: 10000 }, (err, metadata) => {
    //   console.log('getMetadata', err, metadata)
    // })

    // const metadataSync = await this.client.getMetadataSync({ topic: this.topicList[0], timeout: 10000 })
    const metadataSync = await this.client.getMetadataSync({ timeout: 10000 })
    console.log('metadataSync', metadataSync)

    const isConnected = this.client.isConnected()
    console.log('isConnected', isConnected)

    const connectedTime = this.client.connectedTime()
    console.log('connectedTime', connectedTime)

    // consume 'ready' event
    this.client.on('ready', arg => {
      console.log(`onReady: ${JSON.stringify(arg)} - with start time: ${this.stat.start}`)
    })
    // // consume 'message' event
    // this.client.on('message', message => console.log(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`))
    // // consume 'batch' event
    // this.client.on('batch', message => console.log(`onBatch: ${JSON.stringify(message)}`))
    this.client.on('event.stats', eventData => console.log('event.stats:', eventData))
    this.client.on('event.throttle', eventData => console.warn('event.throttle:', eventData))
    super.beforeAll()
    this.stat.start = null
  }

  async run () {
    // eslint-disable-next-line
    return new Promise(runResolve => {
      this.client.consume((error, message) => {
        return new Promise((resolve, reject) => {
          if (!this.stat.start) this.stat.start = performance.now()
          if (error) {
            console.error(error)
            // resolve(false)
            reject(error)
          }
          this.opts.debug && console.log(`this.maxMessages=${this.maxMessages} === this.stat.count=${this.stat.count}`)
          if (message) { // check if there is a valid message coming back
            // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
            if (Array.isArray(message) && message?.length > 0) {
              this.opts.debug && console.log(`message.length=${message.length}`)
              for (const msg of message) {
                this.stat.count++
                this.opts.debug && console.log(`Message received[${msg.value.id}] - offset=${msg.offset}`)
                if (!this.consumerConf.rdkafkaConf['enable.auto.commit'] && this.consumerConf.options.sync) {
                  this.client.commitMessageSync(msg)
                } else if (!this.consumerConf.rdkafkaConf['enable.auto.commit']) {
                  this.client.commitMessage(msg)
                }
              }
            } else {
              this.stat.count++
              this.opts.debug && console.log(`Message received[${message.value.id}] - offset=${message.offset}`)
              if (!this.consumerConf.rdkafkaConf['enable.auto.commit'] && this.consumerConf.options.sync) {
                this.client.commitMessageSync(message)
              } else if (!this.consumerConf.rdkafkaConf['enable.auto.commit']) {
                this.client.commitMessage(message)
              }
            }
            if (this.maxMessages === (this.stat.count)) {
              console.log('MAX MESSAGES REACHED! Exiting Consumer...')
              runResolve(true)
            }
            resolve(true)
          } else {
            resolve(false)
          }
        })
      })

      this.client.on('partition.eof', eof => {
        this.opts.debug && console.log(`onEof: ${JSON.stringify(eof)}`)
        runResolve(true)
      })
    })
  }

  async afterAll () {
    console.log(`test:${this.opts.name}::afterAll`)
    super.afterAll()
    console.log('Disconnecting Consumer!')
    await this.client.disconnect()
    // this.client.disconnect()
    console.log({
      consumerConf: inspect({
        options: this.consumerConf?.options,
        rdkafkaConf: this.consumerConf?.rdkafkaConf,
        topicConf: this.consumerConf?.topicConf
      }),
      topicList: inspect(this.topicList)
    })
  }
}

module.exports = Test
