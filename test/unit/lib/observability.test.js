/*****
 License
 --------------
 Copyright © 2020-2026 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Mojaloop Foundation
 - Name Surname <name.surname@mojaloop.io>
 * Shashikant Hirugade <shashi.mojaloop@gmail.com>

*****/

'use strict'

const Test = require('tapes')(require('tape'))
const Sinon = require('sinon')
const rewire = require('rewire')
const Metrics = require('@mojaloop/central-services-metrics')
const logger = require('../../../src/lib/logger').logger

const OBSERVABILITY_PATH = '../../../src/lib/observability'

Metrics.setup({ prefix: '', defaultMetrics: false })

const metricValues = async (name) => {
  const metric = Metrics.getDefaultRegister().getSingleMetric(name)
  return metric ? (await metric.get()).values : []
}

const labelsMatch = (actual, expected) => Object.entries(expected).every(([k, v]) => actual[k] === v)

// Value of a counter/gauge series, or of a histogram's _count/_sum series
const valueOf = async (name, labels, suffix = '') => {
  const values = await metricValues(name)
  const found = values.find(v => (suffix ? v.metricName === `${name}${suffix}` : true) && labelsMatch(v.labels, labels))
  return found ? found.value : 0
}

const stats = ({ assignmentSize = 2, rebalanceCnt = 1, partitions = { 0: { consumer_lag: 5 }, 1: { consumer_lag: 0 } } } = {}) => ({
  cgrp: { assignment_size: assignmentSize, rebalance_cnt: rebalanceCnt },
  topics: {
    'topic-a': {
      partitions: {
        '-1': { consumer_lag: 99 },
        2: { consumer_lag: -1 },
        ...partitions
      }
    }
  }
})

Test('observability', (observabilityTests) => {
  let sandbox
  let obs

  observabilityTests.beforeEach((t) => {
    sandbox = Sinon.createSandbox()
    sandbox.stub(logger, 'warn')
    obs = rewire(OBSERVABILITY_PATH)
    Metrics.getDefaultRegister().resetMetrics()
    t.end()
  })

  observabilityTests.afterEach((t) => {
    sandbox.restore()
    t.end()
  })

  observabilityTests.test('observeConsumedMessage records transit, broker residence and queue wait', async (assert) => {
    const producedAt = 1_000_000
    obs.observeConsumedMessage({ timestamp: producedAt, topic: 'topic-a' }, producedAt + 30, producedAt + 50, 'topic-a', 'group-a')

    const labels = { topic: 'topic-a', consumerGroup: 'group-a' }
    assert.equal(await valueOf('kafka_transit_seconds', labels, '_count'), 1, 'transit observed once')
    assert.equal(await valueOf('kafka_transit_seconds', labels, '_sum'), 0.05, 'transit = handler start - produce')
    assert.equal(await valueOf('kafka_broker_residence_seconds', labels, '_sum'), 0.03, 'broker residence = fetch - produce')
    assert.equal(await valueOf('kafka_consumer_queue_wait_seconds', labels, '_sum'), 0.02, 'queue wait = handler start - fetch')
    assert.end()
  })

  observabilityTests.test('observeConsumedMessage falls back to the message topic, then unknown labels', async (assert) => {
    obs.observeConsumedMessage({ timestamp: 1000, topic: 'from-msg' }, 1010, 1020)
    obs.observeConsumedMessage({ timestamp: 1000 }, 1010, 1020)

    assert.equal(await valueOf('kafka_transit_seconds', { topic: 'from-msg', consumerGroup: 'unknown' }, '_count'), 1, 'message topic used')
    assert.equal(await valueOf('kafka_transit_seconds', { topic: 'unknown', consumerGroup: 'unknown' }, '_count'), 1, 'unknown used when no topic')
    assert.end()
  })

  observabilityTests.test('observeConsumedMessage ignores messages without a usable timestamp', async (assert) => {
    obs.observeConsumedMessage({ timestamp: -1, topic: 't' }, 1010, 1020, 't', 'g')
    obs.observeConsumedMessage({ topic: 't' }, 1010, 1020, 't', 'g')
    obs.observeConsumedMessage(undefined, 1010, 1020, 't', 'g')

    assert.equal(await valueOf('kafka_transit_seconds', { topic: 't' }, '_count'), 0, 'nothing observed')
    assert.equal(await valueOf('kafka_clock_skew_total', { topic: 't' }), 0, 'not counted as skew')
    assert.end()
  })

  observabilityTests.test('observeConsumedMessage counts negative intervals as clock skew instead of observing them', async (assert) => {
    // producer clock ahead of consumer clock: fetch and handler start both "before" produce
    obs.observeConsumedMessage({ timestamp: 2000, topic: 't' }, 1990, 1995, 't', 'g')

    const labels = { topic: 't', consumerGroup: 'g' }
    assert.equal(await valueOf('kafka_clock_skew_total', labels), 2, 'transit and broker residence counted as skew')
    assert.equal(await valueOf('kafka_transit_seconds', labels, '_count'), 0, 'negative transit not observed')
    assert.equal(await valueOf('kafka_consumer_queue_wait_seconds', labels, '_count'), 1, 'local queue wait still observed')
    assert.end()
  })

  observabilityTests.test('observeConsumedMessage ignores non-finite intervals', async (assert) => {
    obs.observeConsumedMessage({ timestamp: 1000, topic: 't' }, undefined, 1020, 't', 'g')

    const labels = { topic: 't', consumerGroup: 'g' }
    assert.equal(await valueOf('kafka_transit_seconds', labels, '_count'), 1, 'transit observed')
    assert.equal(await valueOf('kafka_broker_residence_seconds', labels, '_count'), 0, 'NaN broker residence dropped')
    assert.equal(await valueOf('kafka_clock_skew_total', labels), 0, 'NaN not counted as skew')
    assert.end()
  })

  observabilityTests.test('metrics are not recorded until the host calls Metrics.setup(), and start once it has', async (assert) => {
    const isInitiated = sandbox.stub(Metrics, 'isInitiated').returns(false)
    obs.observeConsumedMessage({ timestamp: 2000, topic: 't' }, 1990, 1995, 't', 'g')
    assert.equal(await valueOf('kafka_clock_skew_total', { topic: 't' }), 0, 'nothing recorded before setup')

    isInitiated.returns(true)
    obs.observeConsumedMessage({ timestamp: 2000, topic: 't' }, 1990, 1995, 't', 'g')
    assert.equal(await valueOf('kafka_clock_skew_total', { topic: 't' }), 2, 'recorded once setup has run (not latched off)')
    assert.end()
  })

  observabilityTests.test('helpers are no-ops when central-services-metrics is not installed', async (assert) => {
    obs.__set__({ metricsLibResolved: true, metricsLib: undefined })
    obs.observeConsumedMessage({ timestamp: 1000, topic: 't' }, 1010, 1020, 't', 'g')
    obs.observeProduceError('t', 'ERR__QUEUE_FULL')

    assert.equal(await valueOf('kafka_transit_seconds', { topic: 't' }, '_count'), 0, 'nothing observed')
    assert.equal(await valueOf('kafka_produce_errors_total', { topic: 't' }), 0, 'nothing counted')
    assert.end()
  })

  observabilityTests.test('a metric that cannot be registered is disabled with one warning, not retried per message', async (assert) => {
    const getHistogram = sandbox.stub(Metrics, 'getHistogram').throws(new Error('already registered'))
    obs.observeConsumedMessage({ timestamp: 1000, topic: 't' }, 1010, 1020, 't', 'g')
    obs.observeConsumedMessage({ timestamp: 1000, topic: 't' }, 1010, 1020, 't', 'g')

    assert.equal(getHistogram.callCount, 3, 'each of the three histograms attempted once')
    assert.equal(logger.warn.callCount, 3, 'one warning per disabled metric')
    assert.end()
  })

  observabilityTests.test('helpers never throw into message handling', (assert) => {
    const broken = { observe: () => { throw new Error('boom') }, inc: () => { throw new Error('boom') } }
    Object.assign(obs.__get__('instances'), { transit: broken, produceAck: broken, produceErrors: broken })

    assert.doesNotThrow(() => obs.observeConsumedMessage({ timestamp: 1000, topic: 't' }, 1010, 1020, 't', 'g'), 'consume path')
    assert.doesNotThrow(() => obs.observeProduceAck('t', Date.now() - 5), 'produce ack path')
    assert.doesNotThrow(() => obs.observeProduceError('t', 'ERR__QUEUE_FULL'), 'produce error path')
    assert.end()
  })

  observabilityTests.test('observeProduceAck records produce-to-ack latency', async (assert) => {
    obs.observeProduceAck('topic-a', Date.now() - 20)
    obs.observeProduceAck(undefined, Date.now() - 20)

    assert.equal(await valueOf('kafka_produce_ack_seconds', { topic: 'topic-a' }, '_count'), 1, 'observed for topic')
    assert.ok(await valueOf('kafka_produce_ack_seconds', { topic: 'topic-a' }, '_sum') >= 0.02, 'latency in seconds')
    assert.equal(await valueOf('kafka_produce_ack_seconds', { topic: 'unknown' }, '_count'), 1, 'unknown topic label')
    assert.end()
  })

  observabilityTests.test('observeProduceAck ignores missing timestamps and wall-clock steps', async (assert) => {
    obs.observeProduceAck('t', -1)
    obs.observeProduceAck('t', undefined)
    obs.observeProduceAck('t', Date.now() + 60_000)

    assert.equal(await valueOf('kafka_produce_ack_seconds', { topic: 't' }, '_count'), 0, 'nothing observed')
    assert.equal(await valueOf('kafka_clock_skew_total', { topic: 't' }), 0, 'not counted as cross-host skew')
    assert.end()
  })

  observabilityTests.test('observeProduceError counts failures by topic and error code', async (assert) => {
    obs.observeProduceError('t', 'ERR__QUEUE_FULL')
    obs.observeProduceError('t', 'ERR__QUEUE_FULL')
    obs.observeProduceError(undefined, undefined)

    assert.equal(await valueOf('kafka_produce_errors_total', { topic: 't', code: 'ERR__QUEUE_FULL' }), 2, 'counted by code')
    assert.equal(await valueOf('kafka_produce_errors_total', { topic: 'unknown', code: 'unknown' }), 1, 'unknown labels')
    assert.end()
  })

  observabilityTests.test('stats observer: records assigned partitions and per-partition lag for owned partitions only', async (assert) => {
    const observer = obs.createConsumerStatsObserver('g')
    observer.observe(stats())

    assert.equal(await valueOf('kafka_consumer_assigned_partitions', { consumerGroup: 'g' }), 2, 'assignment size')
    assert.equal(await valueOf('kafka_consumer_lag', { topic: 'topic-a', partition: '0', consumerGroup: 'g' }), 5, 'lag on partition 0')
    assert.equal(await valueOf('kafka_consumer_lag', { topic: 'topic-a', partition: '1', consumerGroup: 'g' }), 0, 'zero lag is still reported')
    const lagPartitions = (await metricValues('kafka_consumer_lag')).filter(v => v.labels.consumerGroup === 'g').map(v => v.labels.partition)
    assert.deepEqual(lagPartitions.sort(), ['0', '1'], 'internal partition -1 and unowned partitions (lag -1) skipped')
    assert.end()
  })

  observabilityTests.test('stats observer: turns the cumulative rebalance count into counter increments', async (assert) => {
    const observer = obs.createConsumerStatsObserver('g')
    observer.observe(stats({ rebalanceCnt: 1 }))
    observer.observe(stats({ rebalanceCnt: 1 }))
    assert.equal(await valueOf('kafka_consumer_rebalances_total', { consumerGroup: 'g' }), 1, 'initial join counted once')
    observer.observe(stats({ rebalanceCnt: 3 }))
    assert.equal(await valueOf('kafka_consumer_rebalances_total', { consumerGroup: 'g' }), 3, 'two further rebalances added')
    assert.end()
  })

  observabilityTests.test('stats observer: removes lag series for partitions that were revoked', async (assert) => {
    const observer = obs.createConsumerStatsObserver('g')
    observer.observe(stats())
    observer.observe(stats({ partitions: { 1: { consumer_lag: 7 } } }))

    const lagPartitions = (await metricValues('kafka_consumer_lag')).filter(v => v.labels.consumerGroup === 'g').map(v => v.labels.partition)
    assert.deepEqual(lagPartitions, ['1'], 'partition 0 series removed')
    assert.equal(await valueOf('kafka_consumer_lag', { partition: '1', consumerGroup: 'g' }), 7, 'partition 1 updated')
    assert.end()
  })

  observabilityTests.test('stats observer: clear() removes this consumer\'s lag and assignment series', async (assert) => {
    const observer = obs.createConsumerStatsObserver('g')
    observer.observe(stats())
    observer.clear()

    assert.equal((await metricValues('kafka_consumer_lag')).filter(v => v.labels.consumerGroup === 'g').length, 0, 'lag series removed')
    assert.equal((await metricValues('kafka_consumer_assigned_partitions')).filter(v => v.labels.consumerGroup === 'g').length, 0, 'assignment series removed')
    assert.end()
  })

  observabilityTests.test('stats observer: tolerates missing or partial stats, and defaults the group label', async (assert) => {
    const observer = obs.createConsumerStatsObserver()
    assert.doesNotThrow(() => {
      observer.observe(undefined)
      observer.observe('not-an-object')
      observer.observe({})
      observer.observe({ cgrp: {}, topics: { t: {} } })
      observer.observe({ topics: { t: { partitions: { 0: null } } } })
    }, 'no throw on partial stats')
    observer.observe({ cgrp: { assignment_size: 1 } })
    assert.equal(await valueOf('kafka_consumer_assigned_partitions', { consumerGroup: 'unknown' }), 1, 'unknown group label')
    assert.end()
  })

  observabilityTests.test('stats observer: is a no-op before Metrics.setup()', async (assert) => {
    sandbox.stub(Metrics, 'isInitiated').returns(false)
    const observer = obs.createConsumerStatsObserver('not-setup')
    observer.observe(stats())
    observer.clear()
    assert.equal((await metricValues('kafka_consumer_lag')).filter(v => v.labels.consumerGroup === 'not-setup').length, 0, 'nothing recorded')
    assert.end()
  })

  observabilityTests.end()
})
