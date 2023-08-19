const s0defaultTest = require('./s0-default')
const s01AutoCommitEnabledTest = require('./s01-auto-commit-enabled')
const s02lz4CompressionTest = require('./s02-lz4-compression')
const s10PartAssignmentCoopSticky = require('./s10-part-assignment-coop-sticky')

const benchRunner = async () => {
  await s0defaultTest()
  await s01AutoCommitEnabledTest()
  await s02lz4CompressionTest()
  await s10PartAssignmentCoopSticky() // NOTE: This should always be last otherwise it may cause an error for the partition assignment!
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = {
    s0defaultTest,
    s01AutoCommitEnabledTest,
    s02lz4CompressionTest,
    s10PartAssignmentCoopSticky
  }
}
