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
 * Valentin Genev <valentin.genev@modusbox.com>

 --------------
 ******/

/**
 * Kafka Messages
 * @module Messages
 */

'use strict'
const base64url = require('base64url')
const parseDataUri = require('parse-data-uri')
const clone = require('clone')
const allowedMimeTypes = ['text/plain', 'application/json', 'application/vnd.interoperability.transfers+json;version=1']

// const Logger = require('@mojaloop/central-services-shared').Logger

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
 * Decoded parsed data URI object
 *
 * @typedef {object} Protocol~DecodedURI
 * @property {string} mimeType - mime type of the data
 * @property {buffer} data - Buffer of the decoded message
 *
 */

/**
 * Allowed mime types
 * @typedef {('text/plain'|'application/json'|'application/vnd.interoperability.transfers+json;version=1')} MimeTypes
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

/**
 * Encodes Payload to base64 encoded data URI
 *
 * @param {buffer\|string} input - Buffer or String
 * @param {MimeTypes} mimeType - mime type of the input
 *
 * @return {string} - Returns base64 encoded data  URI string
 */

const encodePayload = (input, mimeType) => {
  if (allowedMimeTypes.includes(mimeType)) {
    return (input instanceof Buffer)
      ? `data:${mimeType};base64,${base64url(input, 'utf8')}`
      : `data:${mimeType};base64,${base64url(Buffer.from(input), 'utf8')}`
  } else {
    throw new Error(`mime type should be one of the following:${allowedMimeTypes.map(e => ` ${e}`)}`)
  }
}

/**
 * Decode Payload to base64 encoded data URI
 *
 * @param {string} input - Data URI or plain string
 * @param {object} [options = {asParsed: true}] - Parising object
 *
 * @return {(object\|Protocol~DecodedURI)} based on the options, returns parsed JSON or decodedURI object
 */

const decodePayload = (input, { asParsed = true } = {}) => {
  const dataUriRegEx = /^\s*data:'?(?:([\w-]+)\/([\w+.-;=]+))'??(?:;charset=([\w-]+))?(?:;(base64))?,(.*)/

  const parseDecodedDataToJson = (decodedData) => {
    if (['application/json', 'application/vnd.interoperability.transfers+json'].includes(decodedData.mimeType)) return JSON.parse(decodedData.data.toString())
    else if (decodedData.mimeType === 'text/plain') return decodedData.data.toString()
    else throw new Error('invalid mime type')
  }

  try {
    if (RegExp(dataUriRegEx).test(input)) {
      let decoded = parseDataUri(input)
      return asParsed
        ? parseDecodedDataToJson(decoded)
        : decoded
    } else if (typeof input === 'string') {
      return asParsed ? JSON.parse(input) : { mimeType: 'text/plain', data: input }
    } else if (typeof input === 'object') {
      return input
    } else {
      throw new Error('input should be Buffer or String')
    }
  } catch (e) {
    throw e
  }
}

/**
 * Decode message or messages
 *
 * @param {(Protocol~Message\|Protocol~Message[])} messages - single message or array of messages with payload encoded as base64 dataURI
 * @param {object} [options = {asParsed: true}] - options to parse the payload or not
 *
 * @returns {(Protocol~Message\|Protocol~Message[])} - messages with decoded payload
 */

const decodeMessages = (messages) => {
  const decodeMessage = (message) => {
    let decodedMessage = clone(message)
    let payload = decodePayload(decodedMessage.value.content.payload)
    decodedMessage.value.content.payload = payload
    return decodedMessage
  }

  if (Array.isArray(messages)) {
    let result = []
    for (let message of messages) {
      let decodedMessage = decodeMessage(message)
      result.push(decodedMessage)
    }
    return result
  } else {
    return decodeMessage(messages)
  }
}

exports.parseMessage = parseMessage
exports.parseCommand = parseCommand
exports.parseNotify = parseNotify
exports.decodePayload = decodePayload
exports.encodePayload = encodePayload
exports.decodeMessages = decodeMessages
