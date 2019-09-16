const Test = require('tapes')(require('tape'))
const Protocol = require('../../../../src/kafka').Protocol
const Logger = require('@mojaloop/central-services-logger')
const Sinon = require('sinon')

const reason = {
  code: 'code',
  description: 'description'
}

const metadata = {
  test: 'test'
}

const parseM = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  metadata: metadata,
  pp: ''
}

const parseMNoMetaData = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  pp: ''
}

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// const parseC = {
//   from: 'from',
//   to: 'to',
//   id: 'key',
//   resource: 'message',
//   type: 'type',
//   metadata: metadata,
//   pp: '',
//   method: 'method',
//   uri: '',
//   status: 'status',
//   reason: {
//     code: 'code',
//     description: 'description'
//   }
// }

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// const parseCNoMetaData = {
//   from: 'from',
//   to: 'to',
//   id: 'key',
//   resource: 'message',
//   type: 'type',
//   pp: '',
//   method: 'method',
//   uri: '',
//   status: 'status',
//   reason: {
//     code: 'code',
//     description: 'description'
//   }
// }

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// const parseN = {
//   from: 'from',
//   to: 'to',
//   id: 'key',
//   type: 'type',
//   metadata: metadata,
//   pp: '',
//   event: 'event',
//   message: 'message',
//   reason: {
//     code: 'code',
//     description: 'description'
//   }
// }

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// const parseNNoMetaData = {
//   from: 'from',
//   to: 'to',
//   id: 'key',
//   type: 'type',
//   pp: '',
//   event: 'event',
//   message: 'message',
//   reason: {
//     code: 'code',
//     description: 'description'
//   }
// }

const purePayload = '{"errorInformation":{"errorCode":"5200","errorDescription":"Generic limit error, amount \u0026 payments threshold."}}'
const rawPayload = Buffer.from(purePayload)

const plainTextDataUri = 'data:text/plain;base64,eyJlcnJvckluZm9ybWF0aW9uIjp7ImVycm9yQ29kZSI6IjUyMDAiLCJlcnJvckRlc2NyaXB0aW9uIjoiR2VuZXJpYyBsaW1pdCBlcnJvciwgYW1vdW50ICYgcGF5bWVudHMgdGhyZXNob2xkLiJ9fQ'
const plainTextDataUriErrorMimeType = 'data:tet/plain;base64,eyJlcnJvckluZm9ybWF0aW9uIjp7ImVycm9yQ29kZSI6IjUyMDAiLCJlcnJvckRlc2NyaXB0aW9uIjoiR2VuZXJpYyBsaW1pdCBlcnJvciwgYW1vdW50ICYgcGF5bWVudHMgdGhyZXNob2xkLiJ9fQ'

const encodedMessage = {
  value: {
    id: 'f2f038cc-b749-464d-a364-c24acad58ef0',
    to: 'mockfsp02',
    from: 'mockfsp01',
    type: 'application/json',
    content: {
      headers: {
        accept: 'application/vnd.interoperability.transfers+json;version=1',
        'content-type': 'application/vnd.interoperability.transfers+json;version=1',
        date: '2019-01-22T21:27:55.000Z',
        'fspiop-source': 'mockfsp01',
        'fspiop-destination': 'mockfsp02',
        'content-length': 437
      },
      payload: 'data:application/json;base64,eyJlcnJvckluZm9ybWF0aW9uIjp7ImVycm9yQ29kZSI6IjUyMDAiLCJlcnJvckRlc2NyaXB0aW9uIjoiR2VuZXJpYyBsaW1pdCBlcnJvciwgYW1vdW50ICYgcGF5bWVudHMgdGhyZXNob2xkLiJ9fQ'
    },
    metadata: {
      event: {
        id: '25240fa4-da6a-4f18-8b42-e391fde70817',
        type: 'prepare',
        action: 'prepare',
        createdAt: '2019-05-06T08:53:16.996Z',
        state: {
          status: 'success',
          code: 0
        }
      }
    }
  }
}

const decodedMessage = {
  value: {
    id: 'f2f038cc-b749-464d-a364-c24acad58ef0',
    to: 'mockfsp02',
    from: 'mockfsp01',
    type: 'application/json',
    content: {
      headers: {
        accept: 'application/vnd.interoperability.transfers+json;version=1',
        'content-type': 'application/vnd.interoperability.transfers+json;version=1',
        date: '2019-01-22T21:27:55.000Z',
        'fspiop-source': 'mockfsp01',
        'fspiop-destination': 'mockfsp02',
        'content-length': 437
      },
      payload: JSON.parse(purePayload)
    },
    metadata: {
      event: {
        id: '25240fa4-da6a-4f18-8b42-e391fde70817',
        type: 'prepare',
        action: 'prepare',
        createdAt: '2019-05-06T08:53:16.996Z',
        state: {
          status: 'success',
          code: 0
        }
      }
    }
  }
}

const messages = [encodedMessage]

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// Test('Protocol::parseCommand', function (assert) {
//   const test = Protocol.parseCommand(parseC)
//   // Logger.debug(test)
//   assert.deepEqual(test, parseC)
//   assert.end()
// })
//
// Test('Protocol::parseCommand - no metadata', function (assert) {
//   const test = Protocol.parseCommand(parseCNoMetaData)
//   // Logger.debug(test)
//   assert.deepEqual(test, parseC)
//   assert.end()
// })

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// Test('Protocol::parseCommand - no params', function (assert) {
//   try {
//     Protocol.parseCommand()
//   } catch (error) {
//     if (error) {
//       Logger.error(error)
//     }
//     assert.ok(Sinon.match(error.message, 'Invalid input params'))
//     assert.end()
//   }
// })

Test('Protocol::parseMessage', function (assert) {
  const test = Protocol.parseMessage(parseM)
  // Logger.debug(test)
  assert.deepEqual(test, parseM)
  assert.end()
})

Test('Protocol::parseMessage - no metadata', function (assert) {
  const test = Protocol.parseMessage(parseMNoMetaData)
  // Logger.debug(test)
  assert.deepEqual(test, parseMNoMetaData)
  assert.end()
})

Test('Protocol::parseMessage - no params', function (assert) {
  try {
    Protocol.parseMessage()
  } catch (error) {
    if (error) {
      Logger.error(error)
    }
    assert.ok(Sinon.match(error.message, 'Invalid input params'))
    assert.end()
  }
})

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// Test('Protocol::parseNotify', function (assert) {
//   const test = Protocol.parseNotify(parseN)
//   // Logger.debug(test)
//   assert.deepEqual(test, parseN)
//   assert.end()
// })

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// Test('Protocol::parseNotify - no metadata', function (assert) {
//   const test = Protocol.parseNotify(parseNNoMetaData)
//   // Logger.debug(test)
//   assert.deepEqual(test, parseNNoMetaData)
//   assert.end()
// })

// TODO: Need to remove this in future once we clarify if command messages will ever be used as part of streaming capability
// Test('Protocol::parseNotify - no params', function (assert) {
//   try {
//     Protocol.parseNotify()
//   } catch (error) {
//     if (error) {
//       Logger.error(error)
//     }
//     assert.ok(Sinon.match(error.message, 'Invalid input params'))
//     assert.end()
//   }
// })

Test('Protocol::parseValue', function (assert) {
  const buf = Buffer.from(JSON.stringify(reason), 'utf8')
  const test = Protocol.parseValue(buf)
  // Logger.debug(test)
  assert.deepEqual(test, reason)
  assert.end()
})

Test('Protocol::parseValue - test JSON parse failure', function (assert) {
  const strMessage = 'not a json message'
  const buf = Buffer.from(strMessage, 'utf8', true)
  const test = Protocol.parseValue(buf)
  Logger.debug(test)
  assert.ok(Sinon.match(test, strMessage))
  assert.end()
})

Test('Protocol::parseValue', function (assert) {
  const buf = Buffer.from(JSON.stringify(reason), 'utf8')
  const test = Protocol.parseValue(buf, 'utf8', false)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, '{"code":"code","description":"description"}'))
  assert.end()
})

Test('Protocol::encodePayload should encode raw data as json', function (assert) {
  const test = Protocol.encodePayload(rawPayload, 'application/json')
  assert.deepEqual(test, encodedMessage.value.content.payload)
  assert.end()
})

Test('Protocol::encodePayload should encode string', function (assert) {
  const test = Protocol.encodePayload(purePayload, 'text/plain')
  assert.equal(test, plainTextDataUri)
  assert.end()
})

Test('Protocol::encodePayload should throw error if mime type is not correct', function (assert) {
  try {
    Protocol.encodePayload(purePayload, 'tex/plain')
    assert.fail('should throw error')
    assert.end()
  } catch (e) {
    assert.ok(e instanceof Error)
    assert.end()
  }
})

Test('Protocol::decodePayload should decode the payload from base64 encoded JSON as dataURI to JSON', function (assert) {
  const test = Protocol.decodePayload(encodedMessage.value.content.payload)
  assert.deepEqual(test, JSON.parse(purePayload))
  assert.end()
})

Test('Protocol::decodePayload should decode the payload from base64 encoded JSON to object with pure data and mimeType properties', function (assert) {
  const test = Protocol.decodePayload(encodedMessage.value.content.payload, { asParsed: false })
  const expectedResults = { mimeType: 'application/json', body: Buffer.from(purePayload) }
  assert.equal(test.mimeType.toString(), expectedResults.mimeType.toString())
  assert.equal(test.body.toString(), expectedResults.body.toString())
  assert.end()
})

Test('Protocol::decodePayload should decode the payload from base64 encoded plain text as dataURI to JSON', function (assert) {
  const test = Protocol.decodePayload(plainTextDataUri)
  assert.deepEqual(JSON.stringify(test), JSON.stringify(purePayload))
  assert.end()
})

Test('Protocol::decodePayload should decode the payload from normal string to JSON', function (assert) {
  const test = Protocol.decodePayload(purePayload)
  assert.deepEqual(test, JSON.parse(purePayload))
  assert.end()
})

Test('Protocol::decodePayload should throw if mime type is not allowed', function (assert) {
  try {
    Protocol.decodePayload(plainTextDataUriErrorMimeType)
    assert.fail('should have thrown error')
    assert.end()
  } catch (e) {
    assert.ok(e instanceof Error)
    assert.end()
  }
})

Test('Protocol::decodePayload should throw if input is not dataURI nor string', function (assert) {
  try {
    Protocol.decodePayload(3)
    assert.fail('should have thrown error')
    assert.end()
  } catch (e) {
    assert.ok(e instanceof Error)
    assert.end()
  }
})

Test('Protocol::decodePayload should decode the payload from normal string to object with mimeType and the string itself ', function (assert) {
  const test = Protocol.decodePayload(purePayload, { asParsed: false })
  assert.deepEqual(JSON.stringify(test), JSON.stringify({ mimeType: 'text/plain', body: purePayload }))
  assert.end()
})

Test('Protocol::decodeMessages should decode message as single message ', function (assert) {
  const test = Protocol.decodeMessages(JSON.parse(JSON.stringify(encodedMessage)))
  assert.deepEqual(test, decodedMessage)
  assert.end()
})

Test('Protocol::decodeMessages should decode message as single message ', function (assert) {
  const test = Protocol.decodeMessages(messages)
  assert.deepEqual(test, [decodedMessage])
  assert.end()
})
