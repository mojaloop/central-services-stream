const { inspect } = require('util')
const Producer = require('@mojaloop/central-services-stream').Kafka.Producer

const Utils = require('#utils/index')
const Sampler = require('#utils/sampler')

class Test extends Sampler {
  constructor (opts) {
    super(opts)

    // Example serializeFn override
    // eslint-disable-next-line no-unused-vars
    const overrideSerializeFn = (message, opts) => {
      return Buffer.from(JSON.stringify(message), opts.messageCharset)
    }

    this.producerConf = opts?.producerConf || {
      options:
      {
        // pollIntervalMs: 100,
        pollIntervalMs: 50,
        messageCharset: 'utf8',
        sync: true, // Recommended that 'queue.buffering.max.ms'= 0 if this is enabled
        // sync: false,
        serializeFn: null // Use this if you want to use default serializeFn
        // serializeFn: overrideSerializeFn // Use this if you want to override the default serializeFn
      },
      rdkafkaConf: { // Ref: https://developer.confluent.io/tutorials/optimize-producer-throughput/confluent.html
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'test',
        event_cb: true,
        dr_cb: true,
        dr_msg_cb: false,
        'statistics.interval.ms': 0, // Enable event.stats event if value is greater than 0
        // 'statistics.interval.ms': 100, // Enable event.stats event if value is greater than 0
        'compression.codec': 'none', // none, gzip, snappy, lz4, zstd
        // 'compression.codec': 'lz4', // Recommended compression algorithm
        // 'retry.backoff.ms': 100,
        // 'message.send.max.retries': 2,
        'socket.keepalive.enable': true,
        // 'queue.buffering.max.messages': 10
        // 'queue.buffering.max.messages': 10000,
        'queue.buffering.max.messages': 100000,
        // 'queue.buffering.max.messages': 10000000,
        'queue.buffering.max.ms': 0, // This works very well when sync=true, since we are not "lingering" for the producer to wait for a queue build-up to dispatch
        // 'queue.buffering.max.ms': 50,
        // 'batch.num.messages': 10000,
        // 'api.version.request': true
        // Enable this for idempotency
        // 'enable.idempotence': true,
        // 'transactional.id': '1234', // this doesn't work
        // 'transactional.id': true, // this doesn't work
        // 'max.in.flight.requests.per.connection': 5
        // 'queuing.strategy': 'fifo' // Deprecated!!
      },
      topicConf: {
        // 'request.required.acks': 1,
        'request.required.acks': -1,
        partitioner: 'murmur2_random'
      }
    }

    this.topicConf = opts?.topicConf || {
      topicName: 'test'
    }

    this.stat.labels = {
      sync: this.producerConf.options.sync,
      compression: this.producerConf.rdkafkaConf['compression.codec'],
      queueBufferingMaxMessages: this.producerConf.rdkafkaConf['queue.buffering.max.messages']
      // producerConf: this.producerConf,
      // topicConf: this.topicConf
    }
  }

  async beforeAll () {
    console.log(`test:${this.opts.name}::beforeAll`)

    this.client = new Producer(this.producerConf)

    this.client.on('ready', args => console.log('ready:', args))

    const connectionResult = await this.client.connect()

    this.opts.debug && console.log(`Connected result=${connectionResult}`)

    const auditStartPrepareMsg = require('#samples/audit-start-prepare')
    const auditStartFulfilMsg = require('#samples/audit-start-fulfil')

    this.messages = [
      auditStartPrepareMsg,
      auditStartFulfilMsg
    ]

    this.client.on('event.stats', eventData => console.log('event-stats:', eventData))
    this.client.on('event.throttle', eventData => console.warn('event.throttle:', eventData))

    // this.client.on('delivery-report', (err, report) => {
    //   if (err) {
    //     console.error(err)
    //   }
    //   console.log('DeliveryReport: ' + JSON.stringify(report))
    // })
    super.beforeAll()
  }

  async run (message) {
    const messageIndex = Utils.randomNumber(0, this.messages.length - 1)

    const messageProtocol = this.messages[messageIndex]

    const newTopicConf = {
      ...this.topicConf,
      ...{
        key: messageProtocol.id,
        opaqueKey: messageProtocol.id
      }
    }

    const offset = await this.client.sendMessage(messageProtocol, newTopicConf)

    this.opts.debug && console.log(`Message sent[${messageProtocol.id}] - offset=${offset}`)
    this.stat.count++
  }

  async afterAll () {
    console.log(`test:${this.opts.name}::afterAll`)
    super.afterAll()
    await this.client.disconnect()
    console.log({
      producerConf: inspect({
        options: this.producerConf?.options,
        rdkafkaConf: this.producerConf?.rdkafkaConf,
        topicConf: this.producerConf?.topicConf
      }),
      topicConf: inspect(this.topicConf)
    })
  }
}

module.exports = Test
