'use strict'
const base64url = require('base64url')
const parseDataUri = require('parse-data-uri')

exports.toDataUri = (contentType, buffer) => {
  return `data:${contentType};base64,${base64url(buffer, 'utf8')}`
}

exports.fromDataUri = (buffer) => {
  return parseDataUri(buffer)
}
