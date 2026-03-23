/**
 * @typedef {object} CommonKafkaOptions - options shared by consumer and producer
 * @prop {string} [messageCharset='utf8'] - encoding for message value
 * @prop {number} [healthCheckPollInterval=25000] - health check frequency in ms
 */

/**
 * @typedef {object} ConsumerKafkaOptions - consumer-specific options
 * @prop {boolean} [sync=false] - use ordered async.queue for sequential message processing
 * @prop {number} [mode=2] - processing mode: 0 (flow), 1 (poll), 2 (recursive)
 * @prop {number} [batchSize=1] - messages per batch (poll/recursive modes)
 * @prop {number} [pollFrequency=10] - poll interval in ms (poll mode)
 * @prop {number} [recursiveTimeout=100] - recursive call timeout in ms (recursive mode)
 * @prop {number} [syncConcurrency=1] - queue concurrency when sync=true
 * @prop {boolean} [syncSingleMessage=false] - process each message individually in sync+recursive mode
 * @prop {boolean} [messageAsJSON=true] - auto-parse JSON messages
 * @prop {Function} [deserializeFn] - custom deserializer: (msgValue, options) => any
 * @prop {number} [consumeTimeout=1000] - rdkafka consume timeout in ms
 * @prop {boolean} [disableOtelSpanAutoCreation=false] - skip OTel span creation
 * @prop {boolean} [otelSpanPerMessage=false] - create OTel span per message in batch
 */

/**
 * @typedef {object} ProducerKafkaOptions - producer-specific options
 * @prop {boolean} [sync=false] - use HighLevelProducer with delivery callbacks instead of Producer
 * @prop {number} [pollIntervalMs=50] - producer poll interval in ms
 * @prop {Function} [serializeFn] - custom serializer: (message, options) => Buffer|string
 */

/** @typedef {CommonKafkaOptions & ConsumerKafkaOptions & ProducerKafkaOptions} KafkaOptions */

/**
 * @typedef {object} LagMonitorConfig - producer lag monitoring
 * @prop {string} topic - topic to monitor
 * @prop {string} consumerGroup - consumer group for lag calculation
 * @prop {number} [interval=5] - lag check interval in seconds
 * @prop {number} [timeout=5] - lag query timeout in seconds
 * @prop {number} [max] - max allowed lag before error
 */

/** @typedef {import('node-rdkafka').ConsumerGlobalConfig | import('node-rdkafka').ProducerGlobalConfig} RdkafkaConf - librdkafka global configuration */
/** @typedef {import('node-rdkafka').ConsumerTopicConfig | import('node-rdkafka').ProducerTopicConfig} TopicConf - per-topic librdkafka configuration */

/**
 * @typedef {object} KafkaConfig - Kafka client configuration
 * @prop {RdkafkaConf} [rdkafkaConf] - librdkafka global configuration
 * @prop {TopicConf} [topicConf] - per-topic configuration
 * @prop {KafkaOptions} [options] - behavioral options for consumer/producer
 * @prop {LagMonitorConfig} [lagMonitor] - producer lag monitoring config
 * @prop {object} [logger] - logger instance
 */

const insertDots = object => Object.fromEntries(
  Object.entries(object).map(([key, value]) => [key.replace(/[A-Z]/g, s => '.' + s.toLowerCase()), value])
)

/**
 * Transform Kafka config by converting CamelCase keys to dot-notation
 * in rdkafkaConf and topicConf objects.
 *
 * @param {KafkaConfig} config - raw Kafka configuration
 * @returns {KafkaConfig} config with dot-notation keys in rdkafkaConf and topicConf
 */
module.exports = config => config && ({
  ...config,
  rdkafkaConf: config.rdkafkaConf && insertDots(config.rdkafkaConf),
  topicConf: config.topicConf && insertDots(config.topicConf)
})
