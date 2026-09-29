const Test = require('tapes')(require('tape'))
const Sinon = require('sinon')
const { parseStats, trackConnectionHealth } = require('../../../src/kafka/shared')
const kafkaBrokerStates = require('../../../src/constants').kafkaBrokerStates

Test('trackConnectionHealth', (t) => {
  let sandbox
  let logger

  t.beforeEach((t) => {
    sandbox = Sinon.createSandbox()
    logger = require('../../../src/lib/logger').logger
    sandbox.stub(logger, 'isDebugEnabled').value(true)
    sandbox.stub(logger, 'isErrorEnabled').value(true)
    sandbox.stub(logger, 'debug')
    sandbox.stub(logger, 'error')
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
    assert.equal(trackConnectionHealth(eventData, logger), true)
    assert.end()
  })

  t.test('returns true when all brokers are UPDATE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UPDATE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), true)
    assert.end()
  })

  t.test('returns false if any broker is DOWN', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.end()
  })

  t.test('returns false if any broker is in TRY_CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.TRY_CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/TRANSITION/), 'logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in CONNECT', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.CONNECT, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/TRANSITION/), 'logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in SSL_HANDSHAKE', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.SSL_HANDSHAKE, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/TRANSITION/), 'logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH_LEGACY', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH_LEGACY, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/TRANSITION/), 'logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in AUTH', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.AUTH, nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/TRANSITION/), 'logger.debug called for TRANSITION')
    assert.end()
  })

  t.test('returns false if any broker is in UNKNOWN state', (assert) => {
    const eventData = {
      brokers: {
        1: { state: 'SOME_UNKNOWN_STATE', nodename: 'broker1', nodeid: 1 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.debug.calledWithMatch(/UNKNOWN state/), 'logger.debug called for UNKNOWN state')
    assert.end()
  })

  t.test('returns false if stats.brokers is missing', (assert) => {
    assert.equal(trackConnectionHealth({}, logger), false)
    assert.end()
  })

  t.test('returns false if eventData is not an object', (assert) => {
    assert.equal(trackConnectionHealth(null, logger), false)
    assert.equal(trackConnectionHealth(undefined, logger), false)
    assert.end()
  })

  t.test('parses eventData if it is a JSON string', (assert) => {
    const eventData = JSON.stringify({
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 }
      }
    })
    assert.equal(trackConnectionHealth(eventData, logger), true)
    assert.end()
  })

  t.test('returns false and logs error if JSON parsing fails', (assert) => {
    assert.equal(trackConnectionHealth('{invalid json', logger), false)
    assert.ok(logger.error.called, 'logger.error should be called')
    assert.end()
  })

  t.test('returns true if brokers object is empty', (assert) => {
    assert.equal(trackConnectionHealth({ brokers: {} }, logger), true)
    assert.end()
  })

  t.test('returns true when all brokers are UP after a state change', (assert) => {
    const eventData = {
      brokers: {
        1: { state: kafkaBrokerStates.UP, nodename: 'broker1', nodeid: 1 },
        2: { state: kafkaBrokerStates.DOWN, nodename: 'broker2', nodeid: 2 }
      }
    }
    assert.equal(trackConnectionHealth(eventData, logger), false, 'Should be false when a broker is DOWN')
    eventData.brokers['2'].state = kafkaBrokerStates.UP
    assert.equal(trackConnectionHealth(eventData, logger), true, 'Should be true when all brokers are UP')
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
    assert.equal(trackConnectionHealth(eventData, logger), true)
    assert.end()
  })

  t.test('returns false and logs error if stats.message JSON parsing fails', (assert) => {
    const eventData = {
      message: '{invalid json'
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.ok(logger.error.calledWithMatch(/error parsing nested stats\.message/), 'logger.error should be called for nested stats.message')
    assert.end()
  })

  t.test('returns false if stats.message is a string but not JSON and brokers is missing', (assert) => {
    const eventData = {
      message: '"not a brokers object"'
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.end()
  })

  t.test('returns false if stats.message is a JSON string with brokers missing', (assert) => {
    const eventData = {
      message: JSON.stringify({ foo: 'bar' })
    }
    assert.equal(trackConnectionHealth(eventData, logger), false)
    assert.end()
  })

  t.test('accepts stats already returned by parseStats', (assert) => {
    const stats = parseStats(JSON.stringify({ brokers: { 1: { state: kafkaBrokerStates.UP } } }), logger)
    assert.equal(trackConnectionHealth(stats, logger), true)
    assert.end()
  })
  t.end()
})

Test('parseStats', (t) => {
  let sandbox
  let logger

  t.beforeEach((t) => {
    sandbox = Sinon.createSandbox()
    logger = require('../../../src/lib/logger').logger
    sandbox.stub(logger, 'error')
    t.end()
  })

  t.afterEach((t) => {
    sandbox.restore()
    t.end()
  })

  t.test('parses a JSON string', (assert) => {
    assert.deepEqual(parseStats('{"cgrp":{"assignment_size":2}}', logger), { cgrp: { assignment_size: 2 } })
    assert.end()
  })

  t.test('returns an object as-is', (assert) => {
    const stats = { cgrp: { assignment_size: 2 } }
    assert.equal(parseStats(stats, logger), stats)
    assert.end()
  })

  t.test('unwraps stats JSON in the message property', (assert) => {
    assert.deepEqual(parseStats({ message: '{"topics":{}}' }, logger), { topics: {} })
    assert.end()
  })

  t.test('returns undefined and logs on invalid JSON', (assert) => {
    assert.equal(parseStats('{notjson', logger), undefined)
    assert.ok(logger.error.calledWithMatch(/error parsing stats/), 'logger.error called')
    assert.end()
  })

  t.test('returns undefined and logs on invalid nested message JSON', (assert) => {
    assert.equal(parseStats({ message: '{notjson' }, logger), undefined)
    assert.ok(logger.error.calledWithMatch(/error parsing nested stats\.message/), 'logger.error called')
    assert.end()
  })

  t.test('uses the default logger when none is given', (assert) => {
    assert.equal(parseStats('{notjson'), undefined)
    assert.ok(logger.error.called, 'default logger used')
    assert.end()
  })
  t.end()
})
