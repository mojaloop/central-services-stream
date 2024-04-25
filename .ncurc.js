module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    "node-rdkafka" // updating to the next major v3.0.0 should be done in a separate task
  ]
}
