'use strict'

const Logger = require('@mojaloop/central-services-logger')

const info = (start, time, tag) => {
  Logger.info(`start[${start}] time[${time}] tag[${tag}]`)
}

const warn = (start, time, tag) => {
  Logger.warn(`start[${start}] time[${time}] tag[${tag}]`)
}

const debug = (start, time, tag) => {
  Logger.warn(`start[${start}] time[${time}] tag[${tag}]`)
}

exports.info = info
exports.warn = warn
exports.debug = debug
