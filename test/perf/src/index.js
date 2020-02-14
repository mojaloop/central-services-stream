/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

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

 * ModusBox
 Miguel de Barros <miguel.debarros@modusbox.com>

 --------------
 ******/

'use strict'

// TO BE DONE
// CLI to start Producer or Consumer
// Also startup HTTP Server to expose /health and /metric end-points
// Instantiate metrics lib

const PJson = require('../package.json')
const Logger = require('@mojaloop/central-services-logger')
const { Command } = require('commander')
const KafkaProducer = require('./kafka/producer')
const KafkaConsumer = require('./kafka/consumer')
const Setup = require('./shared/setup').init
const Config = require('@local/config')

const Program = new Command()

Program
  .version(PJson.version)
  .description(PJson.description)

Program.command('produce') // sub-command name, coffeeType = type, required
  .alias('p') // alternative sub-command is 'o'
  .description('Start Producer') // command description
  .option('--topic <name>', 'Kafka topic')
  .option('--maxMessages <num>', 'Max number of messages to be sent', 1)
  .option('--payloadSize <size>', 'Size of payload to be sent in bytes')
  // .option('--batchSize', 'Start the Prepare Handler')
  .option('--api', 'Enable API')

  // function to execute when command is uses
  .action(async (args) => {
    // Logger.info(`Program.command('produce').args=${Flatted.stringify(args)}`)
    let topic
    let maxMessages
    let payloadSize

    if (args.topic) {
      Logger.info(`Program.command('produce').args.topic=${args.topic}`)
      topic = args.topic
    }

    if (args.maxMessages) {
      Logger.info(`Program.command('produce').args.maxMessages=${args.maxMessages}`)
      try {
        maxMessages = parseInt(args.maxMessages)
      } catch (err) {
        Logger.error(err)
      }
    }

    if (args.payloadSize) {
      Logger.info(`Program.command('produce').args.payloadSize=${args.payloadSize}`)
      try {
        payloadSize = parseInt(args.payloadSize)
      } catch (err) {
        Logger.error(err)
      }
    }

    try {
      await Setup(Config.PRODUCER.HOSTNAME, Config.PRODUCER.PORT, !args.api)
      await KafkaProducer.run(maxMessages, payloadSize, topic)
    } catch (err) {
      Logger.error(err)
    }
  })

Program.command('consume') // sub-command name, coffeeType = type, required
  .alias('p') // alternative sub-command is 'o'
  .description('Start Consumer') // command description
  .option('--batchSize', 'Start the Prepare Handler')
  .option('--api', 'Start the Prepare Handler')

  // function to execute when command is uses
  .action(async (args) => {
    if (args.batchSize) {
      Logger.debug('CLI: Param --batchSize')
    }

    try {
      await Setup(Config.CONSUMER.HOSTNAME, Config.CONSUMER.PORT, !args.api)
      await KafkaConsumer.run()
    } catch (err) {
      Logger.error(err)
    }
  })

// Logger.info(`${JSON.stringify(process.argv)}`)

if (Array.isArray(process.argv) && process.argv.length > 2) {
  // parse command line vars
  Program.parse(process.argv)
} else {
  // display default help
  Program.help()
}
