const s00DefaultTest = require('./s00-default')
const s01AutoCommitEnabledTest = require('./s01-auto-commit-enabled')
const s02Lz4CompressionTest = require('./s02-lz4-compression')
const s03ProducerReducedPollInterval = require('./s03-producer-reduced-poll-interval')
const s04ProducerIncreasedQueueBuffMax = require('./s04-producer-increased-queue-buff-max-')
const s05ConsumerModePoll = require('./s05-consumer-mode-poll')
const s06ConsumerModeFlow = require('./s06-consumer-mode-flow')
const s07ConsumerModeRecursive = require('./s07-consumer-mode-recursive')
const s08ConsumerModeRecursiveWithBatch = require('./s08-consumer-mode-recursive-with-batch')
const s09ProtobufSerilization = require('./s09-protobuf-serilization')
const s10DefaultWithSyncFalse = require('./s10-default-with-sync-false')
const s11PartAssignmentCoopSticky = require('./s11-part-assignment-coop-sticky')

const benchRunner = async () => {
  console.time('timer:benchRunner')
  let statTables = []
  let resultTable = []

  resultTable = await s00DefaultTest()
  statTables = statTables.concat(resultTable)

  resultTable = await s01AutoCommitEnabledTest()
  statTables = statTables.concat(resultTable)

  resultTable = await s02Lz4CompressionTest()
  statTables = statTables.concat(resultTable)

  resultTable = await s03ProducerReducedPollInterval()
  statTables = statTables.concat(resultTable)

  resultTable = await s04ProducerIncreasedQueueBuffMax()
  statTables = statTables.concat(resultTable)

  resultTable = await s05ConsumerModePoll()
  statTables = statTables.concat(resultTable)

  resultTable = await s06ConsumerModeFlow()
  statTables = statTables.concat(resultTable)

  resultTable = await s07ConsumerModeRecursive()
  statTables = statTables.concat(resultTable)

  resultTable = await s08ConsumerModeRecursiveWithBatch()
  statTables = statTables.concat(resultTable)

  resultTable = await s09ProtobufSerilization()
  statTables = statTables.concat(resultTable)

  resultTable = await s10DefaultWithSyncFalse()
  statTables = statTables.concat(resultTable)

  resultTable = await s11PartAssignmentCoopSticky() // NOTE: This should always be last otherwise it may cause an error for the partition assignment!
  statTables = statTables.concat(resultTable)

  console.timeEnd('timer:benchRunner')
  console.table(statTables)
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = {
    s00DefaultTest,
    s01AutoCommitEnabledTest,
    s02Lz4CompressionTest,
    s03ProducerReducedPollInterval,
    s04ProducerIncreasedQueueBuffMax,
    s05ConsumerModePoll,
    s06ConsumerModeFlow,
    s07ConsumerModeRecursive,
    s08ConsumerModeRecursiveWithBatch,
    s11PartAssignmentCoopSticky
  }
}
