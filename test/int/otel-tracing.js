'use strict'

const { env } = require('node:process')
env.OTEL_SERVICE_NAME = 'css-int-tests'
env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://localhost:4318'
env.OTEL_EXPORTER_OTLP_PROTOCOL = 'http/protobuf'
env.OTEL_TRACES_EXPORTER = 'otlp'
require('@opentelemetry/auto-instrumentations-node/register')

const { setTimeout: sleep } = require('node:timers/promises')
const { trace } = require('@opentelemetry/api')

const { Kafka } = require('../../src')
const { Consumer, Producer } = Kafka

const TOPIC = `otel-int-test-${Date.now()}`
const GROUP_ID = `int-test-group-${Date.now()}`
const BROKER = 'localhost:9092'
const MESSAGE_COUNT = 10
const CONSUME_TIMEOUT_MS = 10_000
const FLUSH_WAIT_MS = 5_000

const producerConfig = {
  options: {
    pollIntervalMs: 50,
    messageCharset: 'utf8',
    sync: true
  },
  rdkafkaConf: {
    'metadata.broker.list': BROKER,
    'client.id': 'int-test-producer',
    event_cb: true,
    dr_cb: true,
    'api.version.request': true
  },
  topicConf: {
    'request.required.acks': -1
  }
}

const consumerConfig = {
  options: {
    mode: Consumer.ENUMS.CONSUMER_MODES.recursive,
    batchSize: 7,
    recursiveTimeout: 100,
    messageCharset: 'utf8',
    messageAsJSON: true,
    sync: false,
    consumeTimeout: 500 // 1_000
  },
  rdkafkaConf: {
    'group.id': GROUP_ID,
    'client.id': 'int-test-consumer',
    'metadata.broker.list': BROKER,
    'enable.auto.commit': true,
    event_cb: true,
    'api.version.request': true
  },
  topicConf: {
    'auto.offset.reset': 'earliest'
  }
}

const disconnectAsync = (client) => new Promise(resolve => {
  client.disconnect(resolve)
})

const buildMessageProtocol = (
  index,
  content = { hello: `message-${index}` }
) => ({
  content,
  from: 'int-test-producer',
  to: 'int-test-consumer',
  id: `msg-${index}-${Date.now()}`,
  type: 'application/json',
  metadata: {
    event: {
      type: 'test',
      action: 'produce'
    }
  }
})

const run = async () => {
  const topicConf = { topicName: TOPIC, key: 'test-key', partition: null }

  // -- Producer setup --
  console.log(`[producer] connecting to ${BROKER}...`)
  const producer = new Producer(producerConfig)
  await producer.connect()
  console.log('[producer] connected')

  // -- Produce messages (this also auto-creates the topic) --
  for (let i = 1; i <= MESSAGE_COUNT; i++) {
    await sleep(500)
    const msgProtocol = buildMessageProtocol(i)
    await producer.sendMessage(msgProtocol, topicConf)
    console.log(`[producer] sent message ${i}: ${JSON.stringify(msgProtocol.content)}`)
  }
  console.log(`[producer] all ${MESSAGE_COUNT} messages sent to topic "${TOPIC}"`)

  // Wait for topic metadata to propagate before consumer subscribes
  await sleep(3_000)

  // -- Consumer setup --
  const traceIds = new Set()
  let receivedCount = 0
  let resolveConsumption

  const consumptionDone = new Promise((resolve, reject) => {
    resolveConsumption = resolve
    setTimeout(() => reject(new Error(`Timed out after ${CONSUME_TIMEOUT_MS}ms waiting for ${MESSAGE_COUNT} messages`)), CONSUME_TIMEOUT_MS).unref()
  })

  console.log(`[consumer] connecting to topic "${TOPIC}" with group "${GROUP_ID}"...`)
  const consumer = new Consumer([TOPIC], consumerConfig)
  await consumer.connect()
  console.log('[consumer] connected, starting consumption...')

  const handler = async (error, messages) => {
    await sleep(500) // simulate processing
    if (error) {
      console.error('[consumer] handler error:', error)
      return
    }
    const batch = Array.isArray(messages) ? messages : [messages]

    for (const msg of batch) {
      receivedCount++
      const traceId = trace.getActiveSpan()?.spanContext()?.traceId
      if (traceId) traceIds.add(traceId)
      console.log(`[consumer] received message ${receivedCount}:`, msg.value)
      if (receivedCount >= MESSAGE_COUNT) {
        resolveConsumption()
      }
    }
  }
  consumer.consume(handler)

  // -- Wait for all messages --
  await consumptionDone
  console.log(`[consumer] all ${MESSAGE_COUNT} messages received`)

  // -- Cleanup --
  await disconnectAsync(producer)
  console.log('[producer] disconnected')
  await disconnectAsync(consumer)
  console.log('[consumer] disconnected')

  // -- Wait for BatchSpanProcessor to flush traces to Tempo --
  console.log(`[otel] waiting ${FLUSH_WAIT_MS / 1_000}s for BatchSpanProcessor to flush...`)
  await sleep(FLUSH_WAIT_MS)

  console.log('')
  console.log('=== OTel Tracing Integration Test Complete ===')
  console.log('Check Grafana Tempo at http://localhost:3003 -> Explore -> Tempo -> service.name = css-int-test')
  if (traceIds.size) {
    console.log('')
    console.log(`Collected traceIds (${traceIds.size}):`)
    for (const id of traceIds) {
      console.log(`  - ${id}`)
    }
  }
  console.log('')
}

run()
  .then(() => {
    console.log('Done.')
    process.exit(0)
  })
  .catch((err) => {
    console.error('Integration test failed:', err)
    process.exit(1)
  })
