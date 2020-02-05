const RC = require('parse-strings-in-object')(require('rc')('CSS', require('../../../config/default.json')))

console.log(JSON.stringify(RC))
// Set config object to be returned
const config = {
  HOSTNAME: RC.hostname.replace(/\/$/, ''),
  PORT: RC.port
}

module.exports = config
