const s00defaultTest = require('./s00-default')
const s01AutoCommitEnabledTest = require('./s01-auto-commit-enabled')
const s02lz4CompressionTest = require('./s02-lz4-compression')
const s03ProducerReducedPollInterval = require('./s03-producer-reduced-poll-interval')
const s04ProducerIncreasedQueueBuffMax = require('./s04-producer-increased-queue-buff-max-')
const s10PartAssignmentCoopSticky = require('./s10-part-assignment-coop-sticky')

const benchRunner = async () => {
  await s00defaultTest()
  await s01AutoCommitEnabledTest()
  await s02lz4CompressionTest()
  await s03ProducerReducedPollInterval()
  await s04ProducerIncreasedQueueBuffMax()
  await s10PartAssignmentCoopSticky() // NOTE: This should always be last otherwise it may cause an error for the partition assignment!
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = {
    s00defaultTest,
    s01AutoCommitEnabledTest,
    s02lz4CompressionTest,
    s03ProducerReducedPollInterval,
    s04ProducerIncreasedQueueBuffMax,
    s10PartAssignmentCoopSticky
  }
}
