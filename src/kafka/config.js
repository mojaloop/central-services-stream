module.exports = config => config && ({
  ...config,
  rdkafkaConf: config.rdkafkaConf && Object.fromEntries(Object.entries(config.rdkafkaConf).map(([key, value]) => [key.replace(/[A-Z]/g, s => '.' + s.toLowerCase()), value]))
})
