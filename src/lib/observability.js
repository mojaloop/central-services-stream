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

/**
 * Kafka performance instrumentation (Prometheus).
 *
 * Handler-level histograms start only once a message is already being
 * processed, so the interval between one handler producing a message and the
 * next handler starting on it is not covered by any existing metric. The
 * histograms here measure that interval directly, split into broker residence
 * and local queue wait, alongside produce acknowledgement latency, produce
 * failures, and the consumer-group signals (lag, assignment, rebalances) needed
 * to explain throughput limits under load.
 *
 * The clock source for the consumer-side timings is the message timestamp,
 * which producer.js sets to `Date.now()` at produce time and Kafka preserves
 * under the default CreateTime semantics. That makes those numbers sensitive
 * to clock offset between producer and consumer hosts; `kafka_clock_skew_total`
 * counts samples that came back negative, which is the signal that the offset
 * is large enough to distrust the readings. On a topic configured for
 * LogAppendTime the timestamp is the broker's append time instead, so transit
 * and broker residence would start at the broker rather than at produce.
 *
 * @mojaloop/central-services-metrics is an optional peer dependency, resolved
 * lazily. The package exports a singleton instance, and sharing the host
 * service's instance is what puts these series on the service's existing
 * /metrics endpoint — so they are on exactly when the host has called
 * Metrics.setup(), and every helper is a no-op otherwise. Helpers never throw:
 * instrumentation must not affect message handling.
 */

const { logger } = require('./logger')

// Wider and denser than the central-services-metrics default
// ([0.01, 0.05, 0.1, 0.5, 1, 2, 5]), which has no resolution between 100ms and
// 1s — the range these measurements actually land in.
const BUCKETS = [
  0.001, 0.0025, 0.005, 0.01, 0.02, 0.035, 0.05, 0.075,
  0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 3, 5
]

const DEFS = {
  transit: {
    type: 'histogram',
    name: 'kafka_transit_seconds',
    help: 'Produce timestamp to consumer handler start: full inter-stage transit for one Kafka hop',
    labels: ['topic', 'consumerGroup']
  },
  brokerResidence: {
    type: 'histogram',
    name: 'kafka_broker_residence_seconds',
    help: 'Produce timestamp to message being returned to the client by the broker',
    labels: ['topic', 'consumerGroup']
  },
  queueWait: {
    type: 'histogram',
    name: 'kafka_consumer_queue_wait_seconds',
    help: 'Client fetch to handler start: time spent in the local queue and batch assembly',
    labels: ['topic', 'consumerGroup']
  },
  produceAck: {
    type: 'histogram',
    name: 'kafka_produce_ack_seconds',
    help: 'Produce call to delivery report: client batching (linger.ms), broker write and replication under the configured acks, and up to one producer poll interval (pollIntervalMs)',
    labels: ['topic']
  },
  clockSkew: {
    type: 'counter',
    name: 'kafka_clock_skew_total',
    help: 'Timing samples discarded because the computed interval was negative, indicating clock offset between producer and consumer hosts',
    labels: ['topic', 'consumerGroup']
  },
  produceErrors: {
    type: 'counter',
    name: 'kafka_produce_errors_total',
    help: 'Messages that failed to be enqueued or delivered, by librdkafka error code (e.g. ERR__QUEUE_FULL, ERR__MSG_TIMED_OUT)',
    labels: ['topic', 'code']
  },
  rebalances: {
    type: 'counter',
    name: 'kafka_consumer_rebalances_total',
    help: 'Consumer group rebalances seen by this consumer, from librdkafka statistics (requires statistics.interval.ms > 0)',
    labels: ['consumerGroup']
  },
  assignedPartitions: {
    type: 'gauge',
    name: 'kafka_consumer_assigned_partitions',
    help: 'Partitions currently assigned to this consumer, from librdkafka statistics (requires statistics.interval.ms > 0)',
    labels: ['consumerGroup']
  },
  consumerLag: {
    type: 'gauge',
    name: 'kafka_consumer_lag',
    help: 'Messages between the partition high watermark and this consumer\'s position, from librdkafka statistics (requires statistics.interval.ms > 0)',
    labels: ['topic', 'partition', 'consumerGroup']
  }
}

const FACTORIES = {
  histogram: (m, def) => m.getHistogram(def.name, def.help, def.labels, BUCKETS),
  counter: (m, def) => m.getCounter(def.name, def.help, def.labels),
  gauge: (m, def) => m.getGauge(def.name, def.help, def.labels)
}

let metricsLib
let metricsLibResolved = false
const instances = {}

const getMetricsLib = () => {
  if (!metricsLibResolved) {
    metricsLibResolved = true
    try {
      metricsLib = require('@mojaloop/central-services-metrics')
    } catch (err) {
      metricsLib = undefined
    }
  }
  return typeof metricsLib?.isInitiated === 'function' && metricsLib.isInitiated()
    ? metricsLib
    : undefined
}

const getMetric = (key) => {
  if (instances[key] !== undefined) return instances[key]
  const m = getMetricsLib()
  // Not cached: the host may call Metrics.setup() after this module is first used.
  if (!m) return undefined
  const def = DEFS[key]
  try {
    instances[key] = FACTORIES[def.type](m, def)
  } catch (err) {
    instances[key] = null // cache the failure so the hot path does not retry per message
    logger.warn(`Kafka metric ${def.name} is disabled - it could not be registered: `, err)
  }
  return instances[key]
}

const safely = (fn) => (...args) => {
  try {
    fn(...args)
  } catch (err) {
    // Instrumentation must never break message handling, so this stays
    // silent rather than risking a throw from the logger itself.
  }
}

// librdkafka reports -1 when a message carries no timestamp.
const isTimestamp = (ts) => typeof ts === 'number' && ts > 0

/**
 * Record one consumer-side interval. Negative values mean the two hosts'
 * clocks disagree by more than the interval being measured, so they are
 * counted rather than clamped to zero — clamping would quietly bias every
 * percentile downward.
 */
const observe = (key, labels, ms) => {
  if (!Number.isFinite(ms)) return
  if (ms < 0) {
    const counter = getMetric('clockSkew')
    if (counter) counter.inc(labels)
    return
  }
  const histogram = getMetric(key)
  if (histogram) histogram.observe(labels, ms / 1000)
}

/**
 * @param {object} msg - consumed message, carrying the producer's timestamp
 * @param {number} fetchedAt - epoch ms when the client returned the message
 * @param {number} startedAt - epoch ms when the handler began processing it
 * @param {string} topic
 * @param {string} consumerGroup
 */
const observeConsumedMessage = (msg, fetchedAt, startedAt, topic, consumerGroup) => {
  const producedAt = msg?.timestamp
  if (!isTimestamp(producedAt)) return
  const labels = { topic: topic || msg.topic || 'unknown', consumerGroup: consumerGroup || 'unknown' }
  observe('transit', labels, startedAt - producedAt)
  observe('brokerResidence', labels, fetchedAt - producedAt)
  observe('queueWait', labels, startedAt - fetchedAt)
}

/**
 * @param {string} topic
 * @param {number} producedAt - epoch ms the message was produced (the delivery report's timestamp)
 */
const observeProduceAck = (topic, producedAt) => {
  if (!isTimestamp(producedAt)) return
  const ms = Date.now() - producedAt
  // Same host and clock on both ends, so a negative value is a wall-clock
  // step rather than skew between hosts — drop it.
  if (ms < 0) return
  const histogram = getMetric('produceAck')
  if (histogram) histogram.observe({ topic: topic || 'unknown' }, ms / 1000)
}

/**
 * @param {string} topic
 * @param {string} code - librdkafka error code name, e.g. ERR__QUEUE_FULL
 */
const observeProduceError = (topic, code) => {
  const counter = getMetric('produceErrors')
  if (counter) counter.inc({ topic: topic || 'unknown', code: code || 'unknown' })
}

/**
 * Creates the per-consumer observer for librdkafka statistics. It keeps the
 * state needed to turn librdkafka's cumulative rebalance count into counter
 * increments, and to remove lag series for partitions this consumer no longer
 * owns — otherwise a revoked partition would keep reporting its last lag.
 *
 * @param {string} consumerGroup
 * @returns {{ observe: function(object): void, clear: function(): void }}
 */
const createConsumerStatsObserver = (consumerGroup) => {
  const group = consumerGroup || 'unknown'
  let rebalanceCount = 0
  let lagSeries = new Map()

  const observeStats = (stats) => {
    if (!stats || typeof stats !== 'object') return

    const cgrp = stats.cgrp
    if (cgrp) {
      const assigned = getMetric('assignedPartitions')
      if (assigned && Number.isFinite(cgrp.assignment_size)) assigned.set({ consumerGroup: group }, cgrp.assignment_size)

      if (Number.isFinite(cgrp.rebalance_cnt) && cgrp.rebalance_cnt > rebalanceCount) {
        const rebalances = getMetric('rebalances')
        if (rebalances) rebalances.inc({ consumerGroup: group }, cgrp.rebalance_cnt - rebalanceCount)
        rebalanceCount = cgrp.rebalance_cnt
      }
    }

    const lagGauge = getMetric('consumerLag')
    if (!lagGauge) return
    const current = new Map()
    for (const [topic, topicStats] of Object.entries(stats.topics || {})) {
      for (const [partition, partitionStats] of Object.entries(topicStats?.partitions || {})) {
        // Partition -1 is librdkafka's internal unassigned-partition bucket, and
        // consumer_lag is -1 for partitions this consumer does not own.
        if (partition === '-1' || !(partitionStats?.consumer_lag >= 0)) continue
        const labels = { topic, partition, consumerGroup: group }
        lagGauge.set(labels, partitionStats.consumer_lag)
        current.set(`${topic}/${partition}`, labels)
      }
    }
    for (const [key, labels] of lagSeries) {
      if (!current.has(key)) lagGauge.remove(labels)
    }
    lagSeries = current
  }

  const clear = () => {
    const lagGauge = getMetric('consumerLag')
    if (lagGauge) {
      for (const labels of lagSeries.values()) lagGauge.remove(labels)
    }
    lagSeries = new Map()
    const assigned = getMetric('assignedPartitions')
    if (assigned) assigned.remove({ consumerGroup: group })
  }

  return { observe: safely(observeStats), clear: safely(clear) }
}

module.exports = {
  observeConsumedMessage: safely(observeConsumedMessage),
  observeProduceAck: safely(observeProduceAck),
  observeProduceError: safely(observeProduceError),
  createConsumerStatsObserver,
  BUCKETS
}
