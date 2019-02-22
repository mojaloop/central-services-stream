const Test = require('tapes')(require('tape'))
const Protocol = require('../../../src/kafka').Protocol
const Logger = require('@mojaloop/central-services-shared').Logger
const Sinon = require('sinon')

let reason = {
  code: 'code',
  description: 'description'
}

let metadata = {
  test: 'test'
}

let parseM = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  metadata: metadata,
  pp: ''
}

let parseMNoMetaData = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  pp: ''
}

let parseC = {
  from: 'from',
  to: 'to',
  id: 'key',
  resource: 'message',
  type: 'type',
  metadata: metadata,
  pp: '',
  method: 'method',
  uri: '',
  status: 'status',
  reason: {
    code: 'code',
    description: 'description'
  }
}

let parseCNoMetaData = {
  from: 'from',
  to: 'to',
  id: 'key',
  resource: 'message',
  type: 'type',
  pp: '',
  method: 'method',
  uri: '',
  status: 'status',
  reason: {
    code: 'code',
    description: 'description'
  }
}

let parseN = {
  from: 'from',
  to: 'to',
  id: 'key',
  type: 'type',
  metadata: metadata,
  pp: '',
  event: 'event',
  message: 'message',
  reason: {
    code: 'code',
    description: 'description'
  }
}

let parseNNoMetaData = {
  from: 'from',
  to: 'to',
  id: 'key',
  type: 'type',
  pp: '',
  event: 'event',
  message: 'message',
  reason: {
    code: 'code',
    description: 'description'
  }
}

Test('Protocol::parseCommand', function (assert) {
  let test = Protocol.parseCommand(parseC)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseC))
  assert.end()
})

Test('Protocol::parseCommand - no metadata', function (assert) {
  let test = Protocol.parseCommand(parseCNoMetaData)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseC))
  assert.end()
})

Test('Protocol::parseCommand - no params', function (assert) {
  try {
    Protocol.parseCommand()
  } catch (error) {
    if (error) {
      Logger.error(error)
    }
    assert.ok(Sinon.match(error.message, 'Invalid input params'))
    assert.end()
  }
})

Test('Protocol::parseMessage', function (assert) {
  let test = Protocol.parseMessage(parseM)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseM))
  assert.end()
})

Test('Protocol::parseMessage - no metadata', function (assert) {
  let test = Protocol.parseMessage(parseMNoMetaData)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseM))
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

Test('Protocol::parseNotify', function (assert) {
  let test = Protocol.parseNotify(parseN)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseN))
  assert.end()
})

Test('Protocol::parseNotify - no metadata', function (assert) {
  let test = Protocol.parseNotify(parseNNoMetaData)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseN))
  assert.end()
})

Test('Protocol::parseNotify - no params', function (assert) {
  try {
    Protocol.parseNotify()
  } catch (error) {
    if (error) {
      Logger.error(error)
    }
    assert.ok(Sinon.match(error.message, 'Invalid input params'))
    assert.end()
  }
})

Test('Protocol::parseValue', function (assert) {
  let buf = Buffer.from(JSON.stringify(reason), 'utf8')
  let test = Protocol.parseValue(buf)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, reason))
  assert.end()
})

Test('Protocol::parseValue - test JSON parse failure', function (assert) {
  let strMessage = 'not a json message'
  let buf = Buffer.from(strMessage, 'utf8', true)
  let test = Protocol.parseValue(buf)
  Logger.debug(test)
  assert.ok(Sinon.match(test, strMessage))
  assert.end()
})

Test('Protocol::parseValue', function (assert) {
  let buf = Buffer.from(JSON.stringify(reason), 'utf8')
  let test = Protocol.parseValue(buf, 'utf8', false)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, '{"code":"code","description":"description"}'))
  assert.end()
})
