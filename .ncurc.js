module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    "node-rdkafka" // need to check how major version (3) is handled
  ]
}
