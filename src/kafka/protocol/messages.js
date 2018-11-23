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
 * Kafka Messages
 * @module Messages
 */

'use strict'

// const Logger = require('../../logger')

/**
 * Notification Protocol Object
 *
 * @typedef {object} Protocol~Message
 * @property {string} from - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} to - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} pp - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} id - UUID
 * @property {object} metadata - Key-value object to store metadata object
 * @property {string} type - Required. String with pattern "^[-\w]+/[-\w.]+(\+\w+)?$"
 * @property {object} message - Required. Payload of message
 *
 */

/**
 * Reason Protocol Object
 *
 * @typedef {object} Protocol~Reason
 * @property {number} code - Required. String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} description - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 *
 */

/**
 * Notification Protocol Object
 *
 * @typedef {object} Protocol~Message
 * @property {string} from - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} to - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} pp - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} id - Required. UUID
 * @property {object} metadata - Key-value object to store metadata object
 * @property {string} event - Required. Enum: ["accepted","validated","authorized","dispatched","received","consumed","failed"]
 * @property {Protocol~Reason} reason @see Protocol~Reason
 * @property {object} content - Payload of message
 *
 */

/**
 * Status Protocol Object
 *
 * @typedef {object} Protocol~Status
 * @property {number} code - Required. String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} description - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 *
 */

/**
 * Command Protocol Object
 *
 * @typedef {object} Protocol~Command
 * @property {string} from - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} to - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} pp - String with pattern "^(?:([^\"&'/:<>@@]{1,1023})@@)?([^/@@]{1,1023})(?:/(.{1,1023}))?$"
 * @property {string} id - Required. UUID
 * @property {object} metadata - Key-value object to store metadata object
 * @property {string} method - Required. Enum: ["get","set","merge","delete","subscribe","unsubscribe","observe"]
 * @property {Protocol~Uri} uri - UUID
 * @property {string} type - String with pattern "^[-\w]+/((json)|([-\w.]+(\+json)))$"
 * @property {object} resource - Payload of message
 * @property {Protocol~Status} status - Required. Enum: ["success","failure"]
 * @property {Protocol~Reason} reason @see Protocol~Reason
 *
 */

/**
 * Parse Protocol Messages
 *
 * Validates and enhances Protocol Message with MetaData
 *
 * @todo Validations for Protocol
 *
 * @param {Protocol~Message} messageProtocol - Message to validate
 * @return {Protocol~Message} - Returns validated Protocol result
 */
const parseMessage = (messageProtocol) => {
  if (messageProtocol) {
    if (!messageProtocol.metadata) {
      messageProtocol.metadata = {}
    }
    messageProtocol.metadata['protocol.createdAt'] = Date.now()
  } else {
    throw Error('Invalid input params')
  }

  return {
    from: messageProtocol.from,
    to: messageProtocol.to,
    id: messageProtocol.id,
    content: messageProtocol.content,
    type: messageProtocol.type,
    metadata: messageProtocol.metadata,
    pp: messageProtocol.pp
  }
}

/**
 * Parse Protocol Notifications
 *
 * Validates and enhances Protocol Notification with MetaData
 *
 * @todo Validations for Protocol
 *
 * @param {Protocol~Notification} messageProtocol - Notification to validate
 * @return {Protocol~Notification} - Returns validated Protocol result
 */
const parseNotify = (messageProtocol) => {
  if (messageProtocol) {
    if (!messageProtocol.metadata) {
      messageProtocol.metadata = {}
    }
    messageProtocol.metadata['protocol.createdAt'] = Date.now()
  } else {
    throw Error('Invalid input params')
  }

  return {
    from: messageProtocol.from,
    to: messageProtocol.to,
    id: messageProtocol.id,
    type: messageProtocol.type,
    metadata: messageProtocol.metadata,
    pp: messageProtocol.pp,
    event: messageProtocol.event,
    message: messageProtocol.message,
    reason: messageProtocol.reason
  }
}

/**
 * Parse Protocol Commands
 *
 * Validates and enhances Protocol Command with MetaData
 *
 * @todo Validations for Protocol
 *
 * @param {Protocol~Command} messageProtocol - Command to validate
 * @return {Protocol~Command} - Returns validated Protocol result
 */
const parseCommand = (messageProtocol) => {
  if (messageProtocol) {
    if (!messageProtocol.metadata) {
      messageProtocol.metadata = {}
    }
    messageProtocol.metadata['protocol.createdAt'] = Date.now()
  } else {
    throw Error('Invalid input params')
  }

  return {
    from: messageProtocol.from,
    to: messageProtocol.to,
    id: messageProtocol.key,
    resource: messageProtocol.resource,
    type: messageProtocol.type,
    metadata: messageProtocol.metadata,
    pp: messageProtocol.pp,
    method: messageProtocol.method,
    uri: messageProtocol.uri,
    status: messageProtocol.status,
    reason: messageProtocol.reason
  }
}

exports.parseMessage = parseMessage
exports.parseCommand = parseCommand
exports.parseNotify = parseNotify
