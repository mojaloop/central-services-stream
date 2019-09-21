'use strict'

const Test = require('tape')

const Index = require('../../src')

Test('Index', indexTest => {
  indexTest.test('Exports Kafka', test => {
    test.equal(Index.Kafka, require('../../src/kafka'))
    test.end()
  })

  indexTest.test('Exports kafka util', test => {
    test.equal(Index.Util, require('../../src/util'))
    test.end()
  })

  indexTest.end()
})
