const RC = require('parse-strings-in-object')(require('rc')('CSS', require('../../../config/default.json')))

// console.log(JSON.stringify(RC))
// Set config object to be returned
const config = {
  PRODUCER: RC.PRODUCER,
  CONSUMER: RC.CONSUMER,
  INSTRUMENTATION_METRICS_DISABLED: RC.INSTRUMENTATION.METRICS.DISABLED,
  INSTRUMENTATION_METRICS_CONFIG: RC.INSTRUMENTATION.METRICS.config
}

module.exports = config
