const { Bench } = require('tinybench')

const TestProducer = require('./scripts/producer')
const TestConsumer = require('./scripts/consumer')

const main = async () => {
  console.time('timer:benchmark::main')

  const testProducer = new TestProducer({
    name: 'Producer',
    debug: process.env.DEBUG || false
  })
  const testConsumer = new TestConsumer({
    name: 'Consumer',
    debug: process.env.DEBUG || false
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
    time: 10 * 1000 // This is the time in milliseconds we want to run the benchmark for.
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
}

main()
