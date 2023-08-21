const { Bench } = require('tinybench')
const ConsumerEnums = require('@mojaloop/central-services-stream').Kafka.Consumer.ENUMS

const TestProducer = require('./scripts/producer')
const TestConsumer = require('./scripts/consumer')

const benchRunner = async () => {
  const envTime = parseInt(process.env.TIME) || 10
  const scenario = module.filename.split(/[\\/]/).pop()
  console.log(`Starting benchmark - ${scenario}, env.TIME=${envTime}`)
  console.time('timer:benchmark::main')

  const producerOpts = {
    producerConf: {
      options:
      {
        pollIntervalMs: 10,
        messageCharset: 'utf8',
        sync: true, // Recommended that 'queue.buffering.max.ms'= 0 if this is enabled
        serializeFn: null // Use this if you want to use default serializeFn
      },
      rdkafkaConf: { // Ref: https://developer.confluent.io/tutorials/optimize-producer-throughput/confluent.html
        'metadata.broker.list': 'localhost:9092',
        'client.id': 'test',
        event_cb: true,
        dr_cb: true,
        'statistics.interval.ms': 0, // Enable event.stats event if value is greater than 0
        'compression.codec': 'none', // Recommended compression algorithm
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 0, // This works very well when sync=true, since we are not "lingering" for the producer to wait for a queue build-up to dispatch
        'api.version.request': true
      },
      topicConf: {
        'request.required.acks': -1,
        partitioner: 'murmur2_random'
      }
    },
    topicConf: {
      topicName: 'test'
    }
  }

  const testProducer = new TestProducer({
    scenario,
    name: 'Producer',
    debug: process.env.DEBUG || false,
    ...producerOpts
  })

  const consumerOpts = {
    consumerConf: {
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
      },
      rdkafkaConf: {
        'client.id': 'test',
        'group.id': 'cl-group-test',
        'metadata.broker.list': 'localhost:9092',
        'statistics.interval.ms': 0, // Enable event.stats event if value is greater than 0
        'socket.keepalive.enable': true,
        'enable.auto.commit': false,
        'auto.commit.interval.ms': 100,
        'allow.auto.create.topics': true,
        'partition.assignment.strategy': 'range,roundrobin',
        'enable.partition.eof': true,
        'api.version.request': true
      },
      topicConf: {
        'auto.offset.reset': 'earliest'
      }
    },
    topicList: [
      'test'
    ]
  }

  const testConsumer = new TestConsumer({
    scenario,
    name: 'Consumer',
    debug: process.env.DEBUG || false,
    ...consumerOpts
  })

  const fnProducerOpts = {
    beforeAll: async () => {
      return testProducer.beforeAll()
    },
    afterAll: async () => {
      return testProducer.afterAll()
    }
  }

  const fnConsumerOpts = {
    beforeAll: async () => {
      return testConsumer.beforeAll()
    },
    afterAll: async () => {
      return testConsumer.afterAll()
    }
  }

  const benchProducerConf = {
    // iterations: 100, // This is how many messages we want to produce.
    // time: 0 // This is set to 0, to guarantee the number of iterations.
    time: envTime * 1000 // This is the time in milliseconds we want to run the benchmark for.
  }
  const benchProducer = new Bench(benchProducerConf)

  benchProducer
    .add('produce', async () => {
      await testProducer.run()
    }, fnProducerOpts)
    .todo('unimplemented bench')

  console.time('timer:benchmark::producer:run')
  await benchProducer.run()
  console.timeEnd('timer:benchmark::producer:run')
  // console.table(benchProducer.table())

  const benchConsumerConf = {
    iterations: 1, // We only want 1 iteration since the consumer will run until the partition.eof event is reached.
    time: 0 // This is set to 0, to guarantee the number of iterations.
  }
  const benchConsumer = new Bench(benchConsumerConf)

  benchConsumer
    .add('consumer', async () => {
      await testConsumer.run()
    }, fnConsumerOpts)
    .todo('unimplemented bench')

  console.time('timer:benchmark::consumer:run')
  await benchConsumer.run()
  console.timeEnd('timer:benchmark::consumer:run')
  console.timeEnd('timer:benchmark::main')

  // We can print the tinybench stats using these commands:
  // console.table(benchProducer.table())
  // console.table(benchConsumer.table())

  console.log('benchProducerConf:', benchProducerConf)
  console.log('benchConsumerConf:', benchConsumerConf)
  console.table(testProducer.getTable())
  console.table(testConsumer.getTable())
  return [].concat(testProducer.getTable(), testConsumer.getTable())
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = benchRunner
}
