const { Bench } = require('tinybench')
const ConsumerEnums = require('@mojaloop/central-services-stream').Kafka.Consumer.ENUMS
const Protocol = require('@mojaloop/central-services-stream').Kafka.Protocol
const protobuf = require('protobufjs')

const TestProducer = require('#scripts/producer')
const TestConsumer = require('#scripts/consumer')

const protoInit = async (file, type) => {
  const root = await protobuf.load(file)

  const message = root.lookupType(type)

  return message
}

const benchRunner = async (opts) => {
  const benchProducerConf = opts?.benchProducerConf || {
    // iterations: 100, // This is how many messages we want to produce.
    // time: 0 // This is set to 0, to guarantee the number of iterations.
    time: (process.env?.TIME || 30) * 1000 // This is the time in milliseconds we want to run the benchmark for.
  }

  const scenario = module.filename.split(/[\\/]/).pop()
  console.log(`Starting benchmark - ${scenario}`, benchProducerConf)

  console.time('timer:benchmark::main')

  const TestMessage = await protoInit(process.cwd() + '/samples/message.proto', 'audit.Protocol')

  // default serializeFn implementation
  // eslint-disable-next-line
  const defaultSerializeFn = (message, opts) => {
    const bufferResponse = Buffer.from(JSON.stringify(message), opts.messageCharset)
    return bufferResponse
  }

  // default deserializeFn implementation
  // eslint-disable-next-line
  const defaultDeserializeFn = (buffer, opts) => {
    return Protocol.parseValue(buffer, opts.messageCharset, opts.messageAsJSON)
  }

  const serializeFn = (message, opts) => {
    // console.log('serializeFn', JSON.stringify(message, null, 2))
    // Verify the payload if necessary (i.e. when possibly incomplete or invalid)
    // const errMsg = TestMessage.verify(message)
    // if (errMsg) {
    //   console.error('Error:', errMsg)
    //   throw errMsg
    // }

    // Create a new message
    const pmessage = TestMessage.create(message) // or use .fromObject if conversion is necessary

    // // Encode a message to an Uint8Array (browser) or Buffer (node)
    const buffer = TestMessage.encode(pmessage).finish()
    return buffer
  }

  const deserializeFn = (buffer, opts) => {
    // Encode a message to an Uint8Array (browser) or Buffer (node)
    const message = TestMessage.decode(buffer)

    // Maybe convert the message back to a plain object
    const object = TestMessage.toObject(message, {
      longs: String,
      enums: String,
      bytes: String,
      keepCase: true,
      defaults: false,
      oneofs: true
      // see ConversionOptions
    })
    // console.log('deserializeFn', JSON.stringify(object, null, 2))
    return object
  }

  const producerOpts = {
    producerConf: {
      options:
      {
        pollIntervalMs: 50,
        messageCharset: 'utf8',
        sync: true, // Recommended that 'queue.buffering.max.ms'= 0 if this is enabled
        // serializeFn: null // Use this if you want to use default serializeFn
        serializeFn
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
        // deserializeFn: null // Use this if you want to use default deserializeFn
        deserializeFn
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
