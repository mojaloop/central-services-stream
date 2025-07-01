const Logger = require('@mojaloop/central-services-logger')
const { kafkaBrokerStates } = require('../constants')

/**
 * Tracks connection health based on eventData.stats and logs broker states.
 * @param {Object|string} eventData - The event data (object or JSON string)
 * @returns {boolean} - True if all brokers are healthy, false otherwise
 */
function trackConnectionHealth (eventData) {
  try {
    const stats = typeof eventData === 'string' ? JSON.parse(eventData) : eventData
    if (stats && stats.brokers) {
      let allHealthy = true
      for (const brokerId in stats.brokers) {
        const broker = stats.brokers[brokerId]
        const state = broker.state
        const nodename = broker.nodename
        const nodeid = broker.nodeid
        Logger.isDebugEnabled && Logger.debug(`Broker ${nodename} (ID: ${nodeid}) state: ${state}`)
        switch (state) {
          case kafkaBrokerStates.UP:
          case kafkaBrokerStates.UPDATE:
            Logger.isDebugEnabled && Logger.debug('  -> Broker is HEALTHY')
            break
          case kafkaBrokerStates.DOWN:
            Logger.isDebugEnabled && Logger.debug('  -> Broker is DOWN')
            allHealthy = false
            break
          case kafkaBrokerStates.TRY_CONNECT:
          case kafkaBrokerStates.CONNECT:
          case kafkaBrokerStates.SSL_HANDSHAKE:
          case kafkaBrokerStates.AUTH_LEGACY:
          case kafkaBrokerStates.AUTH:
            Logger.isDebugEnabled && Logger.debug(`  -> Broker is in TRANSITION (${state})`)
            allHealthy = false
            break
          default:
            Logger.isDebugEnabled && Logger.debug(`  -> Broker is in UNKNOWN state: ${state}`)
            allHealthy = false
        }
      }
      return allHealthy
    }
    return false
  } catch (err) {
    Logger.isErrorEnabled && Logger.error(`Consumer::onEventStats - error parsing stats: ${err}`)
    return false
  }
}

module.exports = {
  trackConnectionHealth
}
