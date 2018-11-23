const Test = require('tapes')(require('tape'))
const Protocol = require('../../../src/kafka').Protocol
const Logger = require('../../../src/logger')
const Sinon = require('sinon')

var reason = {
  code: 'code',
  description: 'description'
}

var metadata = {
  test: 'test'
}

var parseM = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  metadata: metadata,
  pp: ''
}

var parseMNoMetaData = {
  from: 'from',
  to: 'to',
  id: 'key',
  content: 'message',
  type: 'type',
  pp: ''
}

var parseC = {
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

var parseCNoMetaData = {
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

var parseN = {
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

var parseNNoMetaData = {
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
  var test = Protocol.parseCommand(parseC)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseC))
  assert.end()
})

Test('Protocol::parseCommand - no metadata', function (assert) {
  var test = Protocol.parseCommand(parseCNoMetaData)
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
  var test = Protocol.parseMessage(parseM)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, parseM))
  assert.end()
})

Test('Protocol::parseMessage - no metadata', function (assert) {
  var test = Protocol.parseMessage(parseMNoMetaData)
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
  var test = Protocol.parseNotify(parseN)
   // Logger.debug(test)
  assert.ok(Sinon.match(test, parseN))
  assert.end()
})

Test('Protocol::parseNotify - no metadata', function (assert) {
  var test = Protocol.parseNotify(parseNNoMetaData)
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
  var buf = Buffer.from(JSON.stringify(reason), 'utf8')
  var test = Protocol.parseValue(buf)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, reason))
  assert.end()
})

Test('Protocol::parseValue - test JSON parse failure', function (assert) {
  var strMessage = 'not a json message'
  var buf = Buffer.from(strMessage, 'utf8', true)
  var test = Protocol.parseValue(buf)
  Logger.debug(test)
  assert.ok(Sinon.match(test, strMessage))
  assert.end()
})

Test('Protocol::parseValue', function (assert) {
  var buf = Buffer.from(JSON.stringify(reason), 'utf8')
  var test = Protocol.parseValue(buf, 'utf8', false)
  // Logger.debug(test)
  assert.ok(Sinon.match(test, '{"code":"code","description":"description"}'))
  assert.end()
})
