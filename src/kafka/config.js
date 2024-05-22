const insertDots = object => Object.fromEntries(Object.entries(object).map(([key, value]) => [key.replace(/[A-Z]/g, s => '.' + s.toLowerCase()), value]))

module.exports = config => config && ({
  ...config,
  rdkafkaConf: config.rdkafkaConf && insertDots(config.rdkafkaConf),
  topicConf: config.topicConf && insertDots(config.topicConf)
})
