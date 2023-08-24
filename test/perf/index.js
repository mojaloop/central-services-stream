const testCases = [
  require('#scenarios/s00-01-default'),
  require('#scenarios/s00-02-default-producer-sync-false'),
  require('#scenarios/s01-auto-commit-enabled'),
  require('#scenarios/s02-lz4-compression'),
  require('#scenarios/s03-producer-reduced-poll-interval'),
  require('#scenarios/s04-producer-increased-queue-buff-max-'),
  require('#scenarios/s05-consumer-mode-poll'),
  require('#scenarios/s06-01-consumer-mode-flow'),
  require('#scenarios/s06-02-consumer-mode-flow-with-sync-false'),
  require('#scenarios/s07-01-consumer-mode-recursive'),
  require('#scenarios/s07-02-consumer-mode-recursive-with-sync-conc'),
  require('#scenarios/s07-03-consumer-mode-recursive-with-sync-conc-batch'),
  require('#scenarios/s07-04-consumer-mode-recursive-sync-false'),
  require('#scenarios/s08-consumer-mode-recursive-with-batch'),
  require('#scenarios/s09-protobuf-serilization'),
  require('#scenarios/s10-default-with-sync-false'),
  require('#scenarios/s11-part-assignment-coop-sticky')
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
