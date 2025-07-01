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
    assert.equal(trackConnectionHealth(eventData), true)
    assert.end()
  })

  t.test('returns true when all brokers are UPDATE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UPDATE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), true)
    assert.end()
  })

  t.test('returns false if any broker is DOWN', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.end()
  })

  t.test('returns false if any broker is in TRY_CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.TRY_CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in SSL_HANDSHAKE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.SSL_HANDSHAKE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH_LEGACY', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH_LEGACY, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/TRANSITION/), 'Logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in UNKNOWN state', (assert) => {
    const eventData = {
      brokers: {
        1: { state: 'SOME_UNKNOWN_STATE', nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false)
    assert.ok(Logger.debug.calledWithMatch(/UNKNOWN state/), 'Logger.debug called for UNKNOWN state')
    assert.end()
  })

  t.test('returns false if stats.brokers is missing', (assert) => {
    assert.equal(trackConnectionHealth({}), false)
    assert.end()
  })

  t.test('returns false if eventData is not an object', (assert) => {
    assert.equal(trackConnectionHealth(null), false)
    assert.equal(trackConnectionHealth(undefined), false)
    assert.end()
  })

  t.test('parses eventData if it is a JSON string', (assert) => {
    const eventData = JSON.stringify({
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 }
      }
    })
    assert.equal(trackConnectionHealth(eventData), true)
    assert.end()
  })

  t.test('returns false and logs error if JSON parsing fails', (assert) => {
    assert.equal(trackConnectionHealth('{invalid json'), false)
    assert.ok(Logger.error.called, 'Logger.error should be called')
    assert.end()
  })

  t.test('returns true if brokers object is empty', (assert) => {
    assert.equal(trackConnectionHealth({ brokers: {} }), true)
    assert.end()
  })

  t.test('returns true when all brokers are UP after a state change', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData), false, 'Should be false when a broker is DOWN')
    eventData.brokers['2'].state = kafkaBrokerStates.UP
    assert.equal(trackConnectionHealth(eventData), true, 'Should be true when all brokers are UP')
    assert.end()
  })

  t.end()
})
