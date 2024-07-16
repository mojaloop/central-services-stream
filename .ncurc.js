module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    // >3 is not compatible with nodejs version
    "node-rdkafka"
  ]
}
