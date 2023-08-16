const { performance } = require('perf_hooks')

class Sampler {
  constructor (opts) {
    this.opts = opts
    this.stat = {
      name: opts.name || 'Sampler',
      count: 0
    }
  }

  async beforeAll () {
    this.stat.start = performance.now()
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
      name: this.stat.name,
      count: this.stat.count,
      'start (ms)': this.stat.start,
      'end (ms)': this.stat.end,
      'duration (s)': this.stat.duration,
      'ops /(s)': this.stat.ops
    }
    return [table]
  }
}

module.exports = Sampler
