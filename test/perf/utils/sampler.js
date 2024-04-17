const { performance } = require('perf_hooks')

// Common Sampler class to capture start-end, duration, ops, etc
class Sampler {
  constructor (opts) {
    this.opts = opts
    this.stat = {
      name: opts.name || 'Sampler',
      date: new Date().toISOString(),
      scenario: opts.scenario || 'default',
      count: 0
    }
  }

  async beforeAll () {
    this.stat.start = performance.now()
    // console.debug(`Starting: ${this.stat.name}`)
  }

  async afterAll () {
    this.stat.end = performance.now()
    const runDiff = this.stat.end - this.stat.start
    this.stat = {
      ...this.stat,
      ...{
        duration: runDiff / 1000,
        ops: this.stat.count / (runDiff / 1000)
      }
    }
  }

  getTable () {
    const table = {
      scenario: this.stat.scenario,
      name: this.stat.name,
      date: this.stat.date,
      count: this.stat.count,
      'ops /(s)': this.stat.ops,
      'duration (s)': this.stat.duration,
      start: this.stat.start,
      end: this.stat.end,
      labels: this.stat?.labels ? JSON.stringify(this.stat.labels) : ''
    }
    return [table]
  }
}

module.exports = Sampler
