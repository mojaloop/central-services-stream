const rdkafkaConfMockDto = ({
  clientId = 'producer-client',
  brokerList = 'localhost:9092',
  groupId
} = {}) => ({
  'client.id': clientId,
  'metadata.broker.list': brokerList,
  ...(groupId && {
    'group.id': groupId
  })
})

// add typings
const kafkaConfigMockDto = ({
  rdkafkaConf = rdkafkaConfMockDto(),
  options
} = {}) => ({
  rdkafkaConf,
  ...(options && { options })
})

module.exports = {
  rdkafkaConfMockDto,
  kafkaConfigMockDto
}
