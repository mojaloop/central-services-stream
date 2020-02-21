'use strict'

const Logger = require('@mojaloop/central-services-logger')
const KafkaHelper = require('./kafkaHelper')
const Perf4js = require('../shared/perf4js')
const Config = require('@local/config')
const initInstrumentation = require('../shared/setup').initInstrumentation
const Metrics = require('@mojaloop/central-services-metrics')
const uuidv4 = require('uuid/v4')

function setImmediatePromise() {
  return new Promise((resolve) => {
    setImmediate(() => resolve())
  })
}

// const sleep = (ms) => {
//   var unixtime_ms = new Date().getTime()
//   while(new Date().getTime() < unixtime_ms + ms) {}
// }
const runConsumer = async (batchSize, produceToTopic) => {
  // const config = Config.CONSUMER.KAFKA.configs
  const config = batchSize ? { ...Config.CONSUMER.KAFKA.configs, ...{ options: { ...Config.CONSUMER.KAFKA.configs.options, ...{ batchSize } } } } : Config.CONSUMER.KAFKA.configs
  const producerConfig = Config.PRODUCER.KAFKA.configs
  const topics = Config.CONSUMER.KAFKA.topics
  await initInstrumentation()
  const histTimer_runConsumer_func = !!Metrics.isInitiated() && Metrics.getHistogram(
    'consumer_runConsumer_func',
    'Central Services Stream - Performance Test Consumer Runner',
    ['success', 'topics']
  ).startTimer()
  const producerClient = !!produceToTopic && await KafkaHelper.createProducer(producerConfig)

  const consumeFunction = (error, messages) => {
    return new Promise((resolve, reject) => {
      var metricStartNow = (new Date()).getTime()
      const histTimer_runConsumer_consumeFunction = !!Metrics.isInitiated() && Metrics.getHistogram(
        'consumer_runConsumer_consumeFunction',
        'Central Services Stream - Performance Test Consumer Callback Handler',
        ['success', 'topics']
      ).startTimer()

      const hist_runConsumer_msgAppProduceConsumeLatency = !!Metrics.isInitiated() && Metrics.getHistogram(
        'consumer_runconsumer_msg_appproduceconsumelatency',
        'Central Services Stream - MsgAppProduceConsumeLatency = ConsumerStartTime - ProducerMessagePayloadCreationTime',
        ['success', 'topics']
      )
      const hist_runConsumer_msgKafkaProduceConsumeLatency = !!Metrics.isInitiated() && Metrics.getHistogram(
        'consumer_runconsumer_msg_kafkaproduceconsumelatency',
        'Central Services Stream - MsgKafkaProduceConsumeLatency = ConsumerStartTime - ProducerMessagePublishedTime',
        ['success', 'topics']
      )
      const hist_runConsumer_msgProducerPublishLatency = !!Metrics.isInitiated() && Metrics.getHistogram(
        'consumer_runconsumer_msg_producerpublishlatency',
        'Central Services Stream - MsgProducerPublishLatency = ProducerMessagePublishedTime - ProducerMessagePayloadCreationTime',
        ['success', 'topics']
      )
      if (error) {
        Logger.info(`Error consuming message - ${error}`)
        reject(error)
      }
      // sleep(2000)
      if (messages) { // check if there is a valid message comming back
        Logger.debug(`Messages Received by callback function - ${JSON.stringify(messages)}`)

        // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
        if (Array.isArray(messages) && messages.length != null && messages.length > 0) {
          Logger.info(`[cid=${messages[0].value.content.batchId}, batchSize=${messages.length}, topicName=${topics}] ~ Consumer::perf::consumeFunction - Batch - START`)
          Logger.info(`Processing a message batch of size ${messages.length}`)
          messages.forEach(async message => {
            Logger.info(`[cid=${message.value.content.batchId}, messageBatch=${messages.length}, messageSize=${message.length}, topicName=${topics}, tid=${message.value.content.id}] ~ Producer::perf::consumeFunction - Message - START`)
            // c.commitMessage(msg)
            const metricStartPayload = parseInt(message.value.content.metrics.start)
            const metricStartKafkaRead = parseInt(message.timestamp)

            const metricEndNow = (new Date()).getTime()

            const metricTimeDiffFromMessageSendToEnd = metricEndNow - metricStartPayload // Metric: MsgAppProduceConsumeLatency = ConsumerStartTime - ProducerMessagePayloadCreationTime
            hist_runConsumer_msgAppProduceConsumeLatency.observe({ success: true, topics }, metricTimeDiffFromMessageSendToEnd)

            const metricTimeDiffFromDropoffToEnd = metricEndNow - metricStartKafkaRead // Metric: MsgKafkaProduceConsumeLatency = ConsumerStartTime - ProducerMessagePublishedTime
            hist_runConsumer_msgKafkaProduceConsumeLatency.observe({ success: true, topics }, metricTimeDiffFromDropoffToEnd)

            const metricTimeDiffFromMessageSendToDropoff = metricStartKafkaRead - metricStartPayload //Metric: MsgProducerPublishLatency = ProducerMessagePublishedTime - ProducerMessagePayloadCreationTime
            hist_runConsumer_msgProducerPublishLatency.observe({ success: true, topics }, metricTimeDiffFromMessageSendToDropoff)

            Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToDropoff, 'metricTimeDiffFromMessageSendToDropoff')
            Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToEnd, 'metricTimeDiffFromMessageSendToEnd')
            Perf4js.info(metricStartPayload, metricTimeDiffFromDropoffToEnd, 'metricTimeDiffFromDropoffToEnd')
            Logger.info(`[cid=${message.value.content.batchId}, messageBatch=${messages.length}, messageSize=${message.length}, topicName=${topics}, tid=${message.value.content.id}] ~ Producer::perf::consumeFunction - Message - END`)
            if (producerClient) {
              const produceBackStart = (new Date()).getTime()
              const histProduceFeedBackTimerEnd = !!Metrics.isInitiated() && Metrics.getHistogram(
                'consumer_runconsumer_produce_back',
                'Central Services Stream - produce back to another topic',
                ['success', 'topics']
              ).startTimer()
              const newMessage = { ...message.value }
              newMessage.content = { ...message.value.content, ...{ id: uuidv4(), refId: message.value.content.id, metrics: { ...message.value.content.metrics, ...{ start: (new Date()).getTime() } } } }      
              try {
                Logger.info(`[cid=${newMessage.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - START`)
                const r = await producerClient.sendMessage(newMessage, { topicName: produceToTopic })
                Logger.info(`[cid=${newMessage.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - END with result: ${r}`)
                histProduceFeedBackTimerEnd({ success: true, topics })
                const produceBackEnd = (new Date()).getTime()
                const produceBackTime = produceBackEnd - produceBackStart
                Perf4js.info(produceBackStart, produceBackTime, 'metricTimeProduceBack')
                await setImmediatePromise()
              } catch (e) {
                Logger.info(`[cid=${newMessage.value.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - END with error: ${e}`)
                histProduceFeedBackTimerEnd({ success: false, topics })
              }
            }
          })
          Logger.info(`[cid=${messages[0].value.content.batchId}, batchSize=${messages.length}, topicName=${topics}] ~ Consumer::perf::consumeFunction - Batch - END`)
        } else {
          // c.commitMessage(message)
          Logger.info('Processing a single message')
          const message = messages
          Logger.info(`[cid=${message.value.content.batchId}, messageBatch=1, messageSize=${message.length}, topicName=${topics}, tid=${message.value.content.id}] ~ Consumer::perf::consumeFunction - Message - START`)
          const metricStartPayload = parseInt(message.value.content.metrics.start)
          const metricStartKafkaRead = parseInt(message.timestamp)

          const metricEndNow = (new Date()).getTime()

          const metricTimeDiffFromMessageSendToEnd = metricEndNow - metricStartPayload // Metric: MsgAppProduceConsumeLatency = ConsumerStartTime - ProducerMessagePayloadCreationTime
          hist_runConsumer_msgAppProduceConsumeLatency.observe({ success: true, topics }, metricTimeDiffFromMessageSendToEnd)

          const metricTimeDiffFromDropoffToEnd = metricEndNow - metricStartKafkaRead // Metric: MsgKafkaProduceConsumeLatency = ConsumerStartTime - ProducerMessagePublishedTime
          hist_runConsumer_msgKafkaProduceConsumeLatency.observe({ success: true, topics }, metricTimeDiffFromDropoffToEnd)

          const metricTimeDiffFromMessageSendToDropoff = metricStartKafkaRead - metricStartPayload //Metric: MsgProducerPublishLatency = ProducerMessagePublishedTime - ProducerMessagePayloadCreationTime
          hist_runConsumer_msgProducerPublishLatency.observe({ success: true, topics }, metricTimeDiffFromMessageSendToDropoff)

          Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToDropoff, 'metricTimeDiffFromMessageSendToDropoff')
          Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToEnd, 'metricTimeDiffFromMessageSendToEnd')
          Perf4js.info(metricStartPayload, metricTimeDiffFromDropoffToEnd, 'metricTimeDiffFromDropoffToEnd')
          Logger.info(`[cid=${message.value.content.batchId}, messageBatch=1, messageSize=${message.length}, topicName=${topics}] ~ Consumer::perf::consumeFunction - Message - END`)
          if (produceToTopic) {
            const produceBackStart = (new Date()).getTime()
            const histProduceFeedBackTimerEnd = !!Metrics.isInitiated() && Metrics.getHistogram(
              'consumer_runconsumer_produce_back',
              'Central Services Stream - produce back to another topic',
              ['success', 'topics']
            ).startTimer()
            const newMessage = { ...message.value }
            newMessage.content = { ...message.value.content, ...{ id: uuidv4(), refId: message.value.content.id, metrics: { ...message.value.content.metrics, ...{ start: (new Date()).getTime() } } } }
            Logger.info(`[cid=${newMessage.value.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - START`)
            producerClient.sendMessage(newMessage, { topicName: produceToTopic })
              .then(r => {
                Logger.info(`[cid=${newMessage.value.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - END with result: ${r}`)
                histProduceFeedBackTimerEnd({ success: true, topics })
                const produceBackEnd = (new Date()).getTime()
                const produceBackTime = produceBackEnd - produceBackStart
                Perf4js.info(produceBackStart, produceBackTime, 'metricTimeProduceBack')
              })
              .catch(e => {
                Logger.info(`[cid=${newMessage.value.content.batchId}, messageBatch=1, messageSize=${newMessage.length}, topicName=${topics}] ~ Consumer::perf::Produced back - END with error: ${e}`)
                histProduceFeedBackTimerEnd({ success: false, topics })
              })
          }
        }
        const metricEndNow = (new Date()).getTime()
        const metricEndOfCallBack = metricEndNow - metricStartNow
        Perf4js.info(metricStartNow, metricEndOfCallBack, 'metricEndOfCallBack')
        !!Metrics.isInitiated() && histTimer_runConsumer_consumeFunction({ success: true, topics: topics })
        resolve(true)
      } else {
        !!Metrics.isInitiated() && histTimer_runConsumer_consumeFunction({ success: false, topics: topics })
        resolve(false)
      }
    })
  }

  // var c1 = KafkaHelper.createConsu mer(topics1, consumeFunction1, config1)
  const consumerClient = KafkaHelper.createConsumer(topics, consumeFunction, config)

  // wrap consumeFunction1 with per metrics

  // const {
  //   performance,
  //   PerformanceObserver
  // } = require('perf_hooks')

  // const timerfyConsumeFunction = performance.timerify(consumeFunction)

  // const obsTimerfyConsumeFunction = new PerformanceObserver((list) => {
  //   const μs = require('microseconds')
  //   // Logger.warn(list.getEntries()[0].duration);
  //   // obsTimerfyConsumeFunction1.disconnect();
  //   const metricNow = (new Date()).getTime()
  //   const perfObsDuration = list.getEntries()[0].duration
  //   const metricOfPerfObsCallBackConsumerFunction = μs.parse(perfObsDuration).microseconds
  //   Perf4js.info(metricNow, metricOfPerfObsCallBackConsumerFunction, 'metricOfPerfObsCallBackConsumerFunction')
  // })
  // obsTimerfyConsumeFunction.observe({ success: true, topics}, { entryTypes: ['function'] })

  // const consumerClient = KafkaHelper.createConsumer(topics, timerfyConsumeFunction, config)
  !!Metrics.isInitiated() && histTimer_runConsumer_func({ success: true, topics: JSON.stringify(topics) })
  return consumerClient
}

module.exports = {
  run: runConsumer
}
