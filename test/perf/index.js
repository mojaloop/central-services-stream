const testCases = [
  require('./s00-default'),
  require('./s00-default'),
  require('./s01-auto-commit-enabled'),
  require('./s02-lz4-compression'),
  require('./s03-producer-reduced-poll-interval'),
  require('./s04-producer-increased-queue-buff-max-'),
  require('./s05-consumer-mode-poll'),
  require('./s06-01-consumer-mode-flow'),
  require('./s06-02-consumer-mode-flow-with-sync-false'),
  require('./s07-01-consumer-mode-recursive'),
  require('./s07-02-consumer-mode-recursive-with-sync-conc'),
  require('./s07-03-consumer-mode-recursive-with-sync-conc-batch'),
  require('./s07-04-consumer-mode-recursive-sync-false'),
  require('./s08-consumer-mode-recursive-with-batch'),
  require('./s09-protobuf-serilization'),
  require('./s10-default-with-sync-false'),
  require('./s11-part-assignment-coop-sticky')
]

const benchRunner = async () => {
  console.time('timer:benchRunner')
  let statTables = []

  const processTime = process.env?.TIME || 10

  const benchProducerConf = {
    // iterations: 100, // This is how many messages we want to produce.
    // time: 0 // This is set to 0, to guarantee the number of iterations.
    time: (processTime || 30) * 1000 // This is the time in milliseconds we want to run the benchmark for.
  }

  const testCaseRunner = async (testCase, opts) => {
    const resultTable = await testCase(opts)
    statTables = statTables.concat(resultTable)
  }

  for (const testCase of testCases) {
    await testCaseRunner(testCase, {
      benchProducerConf
    })
  }

  console.timeEnd('timer:benchRunner')
  console.table(statTables)
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = testCases
}
