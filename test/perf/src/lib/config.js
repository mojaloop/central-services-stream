const RC = require('parse-strings-in-object')(require('rc')('CSS', require('../../config/default.json')))

// Set config object to be returned
const config = {
  HOSTNAME: RC.HOSTNAME.replace(/\/$/, ''),
  PORT: RC.PORT
}

module.exports = config
