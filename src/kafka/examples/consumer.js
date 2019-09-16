/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the 'License') and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Gates Foundation organization for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Gates Foundation
 - Name Surname <name.surname@gatesfoundation.com>

 * Lazola Lucas <lazola.lucas@modusbox.com>
 * Rajiv Mothilal <rajiv.mothilal@modusbox.com>
 * Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/

'use strict'

const Consumer = require('../').Consumer
const ConsumerEnums = require('../').Consumer.ENUMS
const Logger = require('@mojaloop/central-services-logger')

const testConsumer = async () => {
  console.log('Instantiate consumer')
  const c = new Consumer(['test'], {
    options: {
      mode: ConsumerEnums.CONSUMER_MODES.recursive,
      batchSize: 1,
      recursiveTimeout: 100,
      messageCharset: 'utf8',
      messageAsJSON: true,
      sync: true,
      consumeTimeout: 1000
    },
    rdkafkaConf: {
      'group.id': 'kafka',
      'metadata.broker.list': 'localhost:9092',
      'enable.auto.commit': false
    },
    topicConf: {},
    logger: Logger
  })

  console.log('Connect consumer')
  const connectionResult = await c.connect()

  console.log(`Connected result=${connectionResult}`)

  console.log('Consume messages')

  c.consume((error, message) => {
    return new Promise((resolve, reject) => {
      if (error) {
        console.log(`WTDSDSD!!! error ${error}`)
        // resolve(false)
        reject(error)
      }
      if (message) { // check if there is a valid message coming back
        console.log(`Message Received by callback function - ${JSON.stringify(message)}`)
        // lets check if we have received a batch of messages or single. This is dependant on the Consumer Mode
        if (Array.isArray(message) && message.length != null && message.length > 0) {
          message.forEach(msg => {
            c.commitMessage(msg)
          })
        } else {
          c.commitMessage(message)
        }
        resolve(true)
      } else {
        resolve(false)
      }
      // resolve(true)
    })
  })

  // consume 'ready' event
  c.on('ready', arg => console.log(`onReady: ${JSON.stringify(arg)}`))
  // consume 'message' event
  c.on('message', message => console.log(`onMessage: ${message.offset}, ${JSON.stringify(message.value)}`))
  // consume 'batch' event
  c.on('batch', message => console.log(`onBatch: ${JSON.stringify(message)}`))

  console.log('testConsumer::end')
}

testConsumer()
