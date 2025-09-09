module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    "simple-swizzle", // https://github.com/advisories/GHSA-wwpx-h6g5-c7x6 Malware in v0.2.3
  ]
}
