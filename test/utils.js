const Logger = require('@mojaloop/central-services-logger')

// to use as a wrapper on Tape tests
const tryCatchEndTest = (testFn) => async (t) => {
  try {
    await testFn(t)
  } catch (err) {
    Logger.error(`error in test "${t.name}":  ${err?.stack}`)
    t.fail(`${t.name} failed due to error: ${err?.message}`)
  }
  t.end()
}

module.exports = {
  tryCatchEndTest
}
