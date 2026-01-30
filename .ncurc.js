module.exports = {
  reject: [
    // TODO: Upgrading tape to v5+ causes tests to fail due to assert.end() being called multiple times. Will need to address this! Perhaps even move to Jest?
    "tape",
    "simple-swizzle", // https://github.com/advisories/GHSA-wwpx-h6g5-c7x6 Malware in v0.2.3
    "eslint" // https://github.com/advisories/GHSA-p5wg-g6qr-c7cgis incompatible with standard@17.1.2. ESLint 9 removed several options (extensions, useEslintrc, resolvePluginsRelativeTo) that standard-engine relies on. The standard package requires eslint@^8.41.0
  ]
}
