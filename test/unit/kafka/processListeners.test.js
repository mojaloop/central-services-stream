/*****
 License
 --------------
 Copyright © 2020-2026 Mojaloop Foundation
 The Mojaloop files are made available by the Mojaloop Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Mojaloop Foundation for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Mojaloop Foundation
 - Yevhen Kyriukha

 --------------
 ******/

'use strict'

const Test = require('tapes')(require('tape'))
const { execFileSync } = require('node:child_process')
const path = require('node:path')

const modules = ['consumer', 'producer'].map(name => path.resolve(__dirname, '../../../src/kafka', name))
const processEvents = ['SIGTERM', 'SIGINT', 'SIGHUP', 'SIGBREAK', 'beforeExit', 'exit']
const marker = 'process-listeners:'

Test('Kafka client modules', modulesTest => {
  modulesTest.test('leave the process exit to the application', test => {
    const script = `
      const events = ${JSON.stringify(processEvents)}
      const modules = ${JSON.stringify(modules)}
      const count = () => events.map(event => process.listenerCount(event))
      const before = count()
      modules.forEach(module => require(module))
      console.log(${JSON.stringify(marker)} + JSON.stringify({ before, after: count() }))
    `
    const output = execFileSync(process.execPath, ['-e', script], { encoding: 'utf8' })
    const { before, after } = JSON.parse(output.split('\n').find(line => line.startsWith(marker)).slice(marker.length))

    test.deepEqual(after, before, 'no process listeners are added')
    test.end()
  })

  modulesTest.end()
})
