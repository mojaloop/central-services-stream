const defaultTest = require('./default')

const benchRunner = async () => {
  await defaultTest()
}

if (require.main === module) {
  benchRunner()
} else {
  module.exports = {
    defaultTest
  }
}
