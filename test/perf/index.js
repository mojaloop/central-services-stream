const { Bench } = require('tinybench')

// const Producer = require('@mojaloop/central-services-stream').Kafka.Producer

const TestProducer = require('./scripts/producer')

const TestConsumer = require('./scripts/consumer')

const main = async () => {
  console.time('timer:benchmark::main')

  const testProducer = new TestProducer({
    name: 'Producer',
    debug: false
  })
  const testConsumer = new TestConsumer({
    name: 'Consumer',
    debug: false
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

  const benchProducer = new Bench({
    iterations: 1000,
    time: 0
    // iterations: 100
    // iterations: 1000,
    // time: 10 * 1000,
  })

  benchProducer
    .add('produce', async () => {
      await testProducer.run()
    }, fnProducerOpts)
    .todo('unimplemented bench')

  console.time('timer:benchmark::producer:run')
  await benchProducer.run()
  console.timeEnd('timer:benchmark::producer:run')
  // console.table(benchProducer.table())

  const benchConsumer = new Bench({
    iterations: 1,
    time: 0
    // iterations: 100,
    // iterations: 1000,
    // time: 10 * 1000,
  })

  benchConsumer
    .add('consumer', async () => {
      await testConsumer.run()
    }, fnConsumerOpts)
    .todo('unimplemented bench')

  console.time('timer:benchmark::consumer:run')
  await benchConsumer.run()
  console.timeEnd('timer:benchmark::consumer:run')
  console.timeEnd('timer:benchmark::main')

  // console.table(benchProducer.table())
  // console.table(benchConsumer.table())

  console.table(testProducer.getTable())
  console.table(testConsumer.getTable())
}

main()
