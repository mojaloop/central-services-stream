module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    // Performance issues with node-rdkafka v3.4.1, reverting to v3.3.1
    "node-rdkafka"
  ]
}
