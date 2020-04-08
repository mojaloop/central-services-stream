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

/**
 * Kafka Util
 * @module Util
 */

'use strict'

const Logger = require('@mojaloop/central-services-logger')

/**
 * Parse Value
 *
 * Helper function to convert Kafka Buffer to either a String with a specified encoding, and/or returns the value as a JSON Object
 *
 * @param {buffer} value - Buffer object to be converted
 * @param {string} encoding - Buffer object to be converted
 * @param {boolean} asJSON - True to convert to JSON, False to leave the result as a String
 * @return {object} - Returns either a String or JSON Object
 */
const parseValue = (value, encoding = 'utf8', asJSON = true) => {
  Logger.isSillyEnabled && Logger.silly('Protocol::parseMessage() - start')

  // if (typeof value === 'object') {
  //   return value
  // }

  let parsedValue = value.toString(encoding)

  if (asJSON) {
    try {
      parsedValue = JSON.parse(parsedValue)
    } catch (error) {
      Logger.isWarnEnabled && Logger.warn(`Protocol::parseMessage() - error - unable to parse message as JSON -  ${error}`)
      Logger.isSillyEnabled && Logger.silly('Protocol::parseMessage() - end')
      // throw new Error('unable to parse message as JSON')
    }
  }
  Logger.isSillyEnabled && Logger.silly('Protocol::parseMessage() - end')
  return parsedValue
}

exports.parseValue = parseValue
