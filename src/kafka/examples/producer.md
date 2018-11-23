*Example Consumer*

```JSON

'use strict'

const Producer = require('central-services-shared').Kafka.Producer
const Logger = require('central-services-shared').Logger

var testProducer = async () => {
  console.log('testProducer::start')

  const confg = {
    options:
    {
      pollIntervalMs: 100,
      messageCharset: 'utf8'
    },
    rdkafkaConf: {
      'metadata.broker.list': 'localhost:9092',
      'client.id': 'default-client',
      'event_cb': true,
      'compression.codec': 'none',
      'retry.backoff.ms': 100,
      'message.send.max.retries': 2,
      'socket.keepalive.enable': true,
      'queue.buffering.max.messages': 10,
      'queue.buffering.max.ms': 50,
      'batch.num.messages': 10000,
      'api.version.request': true,
      'dr_cb': true
    },
    topicConf: {
      'request.required.acks': 1
    }
  }
  
  var p = new Producer(confg)
  console.log('testProducer::connect::start')
  var connectionResult = await p.connect()
  console.log('testProducer::connect::end')

  console.log(`Connected result=${connectionResult}`)

  var messageProtocol = {
    content: {
      test: 'test'
    },
    from: 'http://test.local/test1',
    to: 'http://test.local/test2',
    type: 'application/json',
    metadata: {
      thisismeta: 'data'
    }
  }

  var topicConf = {
    topicName: 'test'
  }
  
  console.log('testProducer::sendMessage::start1')
  await p.sendMessage(messageProtocol, topicConf).then(results => {
    console.log(`testProducer.sendMessage:: result:'${JSON.stringify(results)}'`)
  })
  console.log('testProducer::sendMessage::end1')
}

testProducer()
```
