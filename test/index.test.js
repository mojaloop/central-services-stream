'use strict'

const Test = require('tape')

const Index = require('../src')

Test('Index', indexTest => {
  indexTest.test('Exports Kafka', test => {
    test.equal(Index.Kafka, require('../src/kafka'))
    test.end()
  })

  indexTest.end()
})
