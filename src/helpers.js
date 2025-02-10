const { OTEL_HEADERS } = require('./constants')

const extractOtelHeaders = (headers) =>
  (!Array.isArray(headers) || headers.length === 0)
    ? {}
    : headers.reduce((acc, header) => {
      Object.entries(header).forEach(([key, value]) => {
        if (OTEL_HEADERS.includes(key)) acc[key] = value?.toString()
      })
      return acc
    }, {})

module.exports = {
  extractOtelHeaders
}
