const Logger = require('@mojaloop/central-services-logger')
const { kafkaBrokerStates } = require('../constants')

/**
 * Tracks connection health based on eventData.stats and logs broker states.
 * @param {Object|string} eventData - The event data (object or JSON string)
 * @returns {boolean} - True if all brokers are healthy, false otherwise
 */
function trackConnectionHealth (eventData, logger) {
  try {
    let stats = typeof eventData === 'string' ? JSON.parse(eventData) : eventData
    // If stats has a 'message' property, parse it as JSON
    if (stats && typeof stats.message === 'string') {
      try {
        stats = JSON.parse(stats.message)
      } catch (e) {
        Logger.isErrorEnabled && logger.error(`Consumer::onEventStats - error parsing nested stats.message: ${e}`)
        return false
      }
    }
    Logger.isDebugEnabled && logger.debug(`Consumer::onEventStats - stats: ${JSON.stringify(stats)}`)
    if (stats && stats.brokers) {
      let allHealthy = true
      for (const brokerId in stats.brokers) {
        const broker = stats.brokers[brokerId]
        const state = broker.state
        // Use broker.name, broker.nodeid, broker.nodename if available, fallback to brokerId
        const nodename = broker.nodename || broker.name || brokerId
        const nodeid = broker.nodeid !== undefined ? broker.nodeid : brokerId
        Logger.isDebugEnabled && logger.debug(`Broker ${nodename} (ID: ${nodeid}) state: ${state}`)
        switch (state) {
          case kafkaBrokerStates.UP:
          case kafkaBrokerStates.UPDATE:
            Logger.isDebugEnabled && logger.debug('  -> Broker is HEALTHY')
            break
          case kafkaBrokerStates.DOWN:
            Logger.isDebugEnabled && logger.debug('  -> Broker is DOWN')
            allHealthy = false
            break
          case kafkaBrokerStates.TRY_CONNECT:
          case kafkaBrokerStates.CONNECT:
          case kafkaBrokerStates.SSL_HANDSHAKE:
          case kafkaBrokerStates.AUTH_LEGACY:
          case kafkaBrokerStates.AUTH:
            Logger.isDebugEnabled && logger.debug(`  -> Broker is in TRANSITION (${state})`)
            allHealthy = false
            break
          default:
            Logger.isDebugEnabled && logger.debug(`  -> Broker is in UNKNOWN state: ${state}`)
            allHealthy = false
        }
      }
      return allHealthy
    }
    return false
  } catch (err) {
    Logger.isErrorEnabled && logger.error(`Consumer::onEventStats - error parsing stats: ${err}`)
    return false
  }
}

module.exports = {
  trackConnectionHealth
}
