module.exports = {
  reject: [
    // tape 5 breaks the suite (verified on 5.10.2, 2026-07): 16/768 failures, all ".end() already called", in the
    // event-driven Consumer tests (flow/poller/recursive modes). The Kafka stub keeps emitting message/batch events
    // after a test's plan is fulfilled; tape 4 tolerated the extra assert/end calls, tape 5 fails them hard.
    "tape",
    "simple-swizzle" // https://github.com/advisories/GHSA-wwpx-h6g5-c7x6 Malware in v0.2.3
  ]
}
