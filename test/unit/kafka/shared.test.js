const Test = require('tapes')(require('tape'))
const Sinon = require('sinon')
const { trackConnectionHealth } = require('../../../src/kafka/shared')
const kafkaBrokerStates = require('../../../src/constants').kafkaBrokerStates

Test('trackConnectionHealth', (t) => {
  let sandbox
  let Logger

  t.beforeEach((t) => {
    sandbox = Sinon.createSandbox()
    Logger = require('@mojaloop/central-services-logger')
    sandbox.stub(Logger, 'debug')
    sandbox.stub(Logger, 'error')
    sandbox.stub(Logger, 'isDebugEnabled').value(true)
    sandbox.stub(Logger, 'isErrorEnabled').value(true)
    t.end()
  })

  t.afterEach((t) => {
    sandbox.restore()
    t.end()
  })

  t.test('returns true when all brokers are UP', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.UP, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), true)
    assert.end()
  })

  t.test('returns true when all brokers are UPDATE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UPDATE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), true)
    assert.end()
  })

  t.test('returns false if any broker is DOWN', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.end()
  })

  t.test('returns false if any broker is in TRY_CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.TRY_CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in SSL_HANDSHAKE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.SSL_HANDSHAKE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH_LEGACY', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH_LEGACY, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in UNKNOWN state', (assert) => {
    const eventData = {
      brokers: {
        1: { state: 'SOME_UNKNOWN_STATE', nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.debug.calledWithMatch(/UNKNOWN state/), 'Logger.debug called for UNKNOWN state')
    assert.end()
  })

  t.test('returns false if stats.brokers is missing', (assert) => {
    assert.equal(trackConnectionHealth({}, Logger), false)
    assert.end()
  })

  t.test('returns false if eventData is not an object', (assert) => {
    assert.equal(trackConnectionHealth(null, Logger), false)
    assert.equal(trackConnectionHealth(undefined, Logger), false)
    assert.end()
  })

  t.test('parses eventData if it is a JSON string', (assert) => {
    const eventData = JSON.stringify({
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 }
      }
    })
    assert.equal(trackConnectionHealth(eventData, Logger), true)
    assert.end()
  })

  t.test('returns false and logs error if JSON parsing fails', (assert) => {
    assert.equal(trackConnectionHealth('{invalid json', Logger), false)
    assert.ok(Logger.error.called, 'Logger.error should be called')
    assert.end()
  })

  t.test('returns true if brokers object is empty', (assert) => {
    assert.equal(trackConnectionHealth({ brokers: {} }, Logger), true)
    assert.end()
  })

  t.test('returns true when all brokers are UP after a state change', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false, 'Should be false when a broker is DOWN')
    eventData.brokers['2'].state = kafkaBrokerStates.UP
    assert.equal(trackConnectionHealth(eventData, Logger), true, 'Should be true when all brokers are UP')
    assert.end()
  })

  t.test('parses stats.message if it is a JSON string', (assert) => {
    const eventData = {
      message: JSON.stringify({
        brokers: {
          1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 }
        }
      })
    }
    assert.equal(trackConnectionHealth(eventData, Logger), true)
    assert.end()
  })

  t.test('returns false and logs error if stats.message JSON parsing fails', (assert) => {
    const eventData = {
      message: '{invalid json'
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.ok(Logger.error.calledWithMatch(/error parsing nested stats\.message/), 'Logger.error should be called for nested stats.message')
    assert.end()
  })

  t.test('returns false if stats.message is a string but not JSON and brokers is missing', (assert) => {
    const eventData = {
      message: '"not a brokers object"'
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.end()
  })

  t.test('returns false if stats.message is a JSON string with brokers missing', (assert) => {
    const eventData = {
      message: JSON.stringify({ foo: 'bar' })
    }
    assert.equal(trackConnectionHealth(eventData, Logger), false)
    assert.end()
  })
  t.end()
})
