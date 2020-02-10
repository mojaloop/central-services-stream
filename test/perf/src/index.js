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
const Program = new Command()

Program
  .version(PJson.version)
  .description(PJson.description)

Program.command('produce') // sub-command name, coffeeType = type, required
  .alias('p') // alternative sub-command is 'o'
  .description('Start Producer') // command description
  .option('--maxMessages', 'Start the Prepare Handler')
  .option('--messageSize', 'Start the Prepare Handler')
  .option('--batchSize', 'Start the Prepare Handler')
  .option('--enableApi', 'Start the Prepare Handler')

  // function to execute when command is uses
  .action(async (args) => {
    if (args.maxMessages) {
      Logger.debug('CLI: Param --maxMessages')
    }
    if (args.messageSize) {
      Logger.debug('CLI: Param --messageSize')
    }
    if (args.batchSize) {
      Logger.debug('CLI: Param --batchSize')
    }

    // module.exports = Setup.initialize({
    //   service: 'handler',
    //   port: Config.PORT,
    //   modules: [Plugin, MetricPlugin],
    //   runMigrations: false,
    //   handlers: handlerList,
    //   runHandlers: true
    // })
  })

Program.command('consume') // sub-command name, coffeeType = type, required
  .alias('p') // alternative sub-command is 'o'
  .description('Start Consumer') // command description
  .option('--batchSize', 'Start the Prepare Handler')
  .option('--enableApi', 'Start the Prepare Handler')

  // function to execute when command is uses
  .action(async (args) => {
    if (args.batchSize) {
      Logger.debug('CLI: Param --batchSize')
    }

    // module.exports = Setup.initialize({
    //   service: 'handler',
    //   port: Config.PORT,
    //   modules: [Plugin, MetricPlugin],
    //   runMigrations: false,
    //   handlers: handlerList,
    //   runHandlers: true
    // })
  })

if (Array.isArray(process.argv) && process.argv.length > 2) {
  // parse command line vars
  Program.parse(process.argv)
} else {
  // display default help
  Program.help()
}
