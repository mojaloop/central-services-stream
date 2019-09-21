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
