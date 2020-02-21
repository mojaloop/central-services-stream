'use strict'

const Logger = require('@mojaloop/central-services-logger')
const KafkaHelper = require('./kafkaHelper')
const Perf4js = require('../shared/perf4js')
const Config = require('@local/config')
const initInstrumentation = require('../shared/setup').initInstrumentation
const Metrics = require('@mojaloop/central-services-metrics')

// const sleep = (ms) => {
//   var unixtime_ms = new Date().getTime()
//   while(new Date().getTime() < unixtime_ms + ms) {}
// }
const runConsumer = async () => {
  const config = Config.CONSUMER.KAFKA.configs
  const topics = Config.CONSUMER.KAFKA.topics
  await initInstrumentation()
  const histTimer_runConsumer_func = !!Metrics.isInitiated() && Metrics.getHistogram(
    'consumer_runConsumer_func',
    'Central Services Stream - Performance Test Consumer Runner',
    ['success', 'topics']
  ).startTimer()

  const consumeFunction = async (error, messages) => {
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
          messages.forEach(message => {
            Logger.info(`[cid=${message.value.content.batchId}, messageBatch=${messages.length}, messageSize=${message.length}, topicName=${topics}, tid=${message.value.content.id}] ~ Producer::perf::consumeFunction - Message - START`)
            // c.commitMessage(msg)
            const metricStartPayload = parseInt(message.value.content.metrics.start)
            const metricStartKafkaRead = parseInt(message.timestamp)

            const metricEndNow = (new Date()).getTime()

            const metricTimeDiffFromMessageSendToEnd = metricEndNow - metricStartPayload // Metric: MsgAppProduceConsumeLatency = ConsumerStartTime - ProducerMessagePayloadCreationTime
            hist_runConsumer_msgAppProduceConsumeLatency.observe(metricTimeDiffFromMessageSendToEnd)
            
            const metricTimeDiffFromDropoffToEnd = metricEndNow - metricStartKafkaRead // Metric: MsgKafkaProduceConsumeLatency = ConsumerStartTime - ProducerMessagePublishedTime
            hist_runConsumer_msgKafkaProduceConsumeLatency.observe(metricTimeDiffFromDropoffToEnd)

            const metricTimeDiffFromMessageSendToDropoff = metricStartKafkaRead - metricStartPayload //Metric: MsgProducerPublishLatency = ProducerMessagePublishedTime - ProducerMessagePayloadCreationTime
            hist_runConsumer_msgProducerPublishLatency.observe(metricTimeDiffFromMessageSendToDropoff)
          

            Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToDropoff, 'metricTimeDiffFromMessageSendToDropoff')
            Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToEnd, 'metricTimeDiffFromMessageSendToEnd')
            Perf4js.info(metricStartPayload, metricTimeDiffFromDropoffToEnd, 'metricTimeDiffFromDropoffToEnd')
            Logger.info(`[cid=${message.value.content.batchId}, messageBatch=${messages.length}, messageSize=${message.length}, topicName=${topics}, tid=${message.value.content.id}] ~ Producer::perf::consumeFunction - Message - END`)
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
          hist_runConsumer_msgAppProduceConsumeLatency.observe(metricTimeDiffFromMessageSendToEnd)
          
          const metricTimeDiffFromDropoffToEnd = metricEndNow - metricStartKafkaRead // Metric: MsgKafkaProduceConsumeLatency = ConsumerStartTime - ProducerMessagePublishedTime
          hist_runConsumer_msgKafkaProduceConsumeLatency.observe(metricTimeDiffFromDropoffToEnd)

          const metricTimeDiffFromMessageSendToDropoff = metricStartKafkaRead - metricStartPayload //Metric: MsgProducerPublishLatency = ProducerMessagePublishedTime - ProducerMessagePayloadCreationTime
          hist_runConsumer_msgProducerPublishLatency.observe(metricTimeDiffFromMessageSendToDropoff)

          Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToDropoff, 'metricTimeDiffFromMessageSendToDropoff')
          Perf4js.info(metricStartPayload, metricTimeDiffFromMessageSendToEnd, 'metricTimeDiffFromMessageSendToEnd')
          Perf4js.info(metricStartPayload, metricTimeDiffFromDropoffToEnd, 'metricTimeDiffFromDropoffToEnd')
          Logger.info(`[cid=${message.value.content.batchId}, messageBatch=1, messageSize=${message.length}, topicName=${topics}] ~ Consumer::perf::consumeFunction - Message - END`)
        }
        const metricEndNow = (new Date()).getTime()
        const metricEndOfCallBack = metricEndNow - metricStartNow
        Perf4js.info(metricStartNow, metricEndOfCallBack, 'metricEndOfCallBack')
        !!Metrics.isInitiated() && histTimer_runConsumer_consumeFunction({ success: true, topics: topicName })
        resolve(true)
      } else {
        !!Metrics.isInitiated() && histTimer_runConsumer_consumeFunction({ success: false, topics: topicName })
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
  // obsTimerfyConsumeFunction.observe({ entryTypes: ['function'] })

  // const consumerClient = KafkaHelper.createConsumer(topics, timerfyConsumeFunction, config)
  !!Metrics.isInitiated() && histTimer_runConsumer_func({ success: true, topics: JSON.stringify(topics) })
  return consumerClient
}

module.exports = {
  run: runConsumer
}
