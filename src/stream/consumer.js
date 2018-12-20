'use strict'

/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Gates Foundation organization for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.

 * Gates Foundation
 - Name Surname <name.surname@gatesfoundation.com>

 * Pedro Barreto <pedrob@crosslaketech.com> <-- Author
 * Miguel de Barros <miguel.debarros@modusbox.com> <-- Adapted into OSS Stream

 --------------
 ******/

const Logger = require('@mojaloop/central-services-shared').Logger
const Kafka = require('node-rdkafka')
const assert = require('assert')
const EventEmitter = require('events')
const Protocol = require('../kafka/protocol')

class Consumer extends EventEmitter {
  constructor (topics = [], config = {}, handler_fn) {
    super()
    assert.ok(Array.isArray(topics) && topics.length > 0, 'invalid topics')
    assert.ok(handler_fn && typeof (handler_fn) === 'function', 'invalid handler_fn')
    assert.ok(config && config.rdkafkaConf && config.rdkafkaConf['metadata.broker.list'] && config.rdkafkaConf['group.id'],
      "invalid config - missing 'config.rdkafkaConf.metadata.broker.list' or 'config.rdkafkaConf.metadata.group.id'")

    this._broker_list = config.rdkafkaConf['metadata.broker.list']
    this._group_id = config.rdkafkaConf['group.id']
    this._client_id = config.rdkafkaConf['client.id']
    this._topics = topics
    this._handler_fn = handler_fn
    this._auto_offset = config.topicConf['auto.offset.reset']

    Logger.debug(`Consumer[${this._topics}]._broker_list: ${this._broker_list}`)
    Logger.debug(`Consumer[${this._topics}]._group_id: ${this._group_id}`)
    Logger.debug(`Consumer[${this._topics}]._client_id: ${this._client_id}`)
    Logger.debug(`Consumer[${this._topics}]._topics: ${this._topics}`)
    Logger.debug(`Consumer[${this._topics}]._auto_offset: ${this._auto_offset}`)

    // const consumer_id = Math.random().toString(36).substring(2, 6) + Math.random().toString(36).substring(2, 6);

    this._consumer = new Kafka.KafkaConsumer(
      // rdkafkaConf
      {
        'debug': 'all',
        'client.id': this._client_id,
        'group.id': this._group_id,
        'metadata.broker.list': this._broker_list,
        'enable.auto.commit': true,
        'socket.keepalive.enable': true
        // 'offset.store.method': 'broker'
        // 'offset_commit_cb': this._offset_commit_cb.bind(this), // not necessary
        // 'rebalance_cb': this._on_rebalancing_cb.bind(this), // this must be commented out otherwise we need to add our own rebalancing algorithm
        // "broker.version.fallback": "0.10.1.0",
        // "api.version.request": true
      },
      // topicConf
      {
        'auto.offset.reset': this._auto_offset
      }
    )

    this._consumer.on('ready', (consumer_info, consumer_metadata) => {
      Logger.debug(`Consumer[${this._topics}].on_ready - node-rdkafka v${Kafka.librdkafkaVersion} info - ${JSON.stringify(consumer_info)}`)
      Logger.silly(`Consumer[${this._topics}].on_ready - node-rdkafka v${Kafka.librdkafkaVersion} metadata - ${JSON.stringify(consumer_metadata)}`)
      this._consumer_info = consumer_info
      this._consumer_metadata = consumer_metadata

      this._consumer.subscribe(this._topics)
      // this._consumer.consume(1);
      // this.emit("ready");
    })

    // important events
    this._consumer.on('subscribed', this._on_subscribed.bind(this))
    this._consumer.on('data', this._on_data.bind(this))
    this._consumer.on('event.error', this._on_error.bind(this))

    // other events
    this._consumer.on('event.throttle', () => {
      Logger.debug(`Consumer[${this._topics}].on_event_throttle - consumer throttled`)
    })

    this._consumer.on('disconnected', () => {
      Logger.debug(`Consumer[${this._topics}].on_disconnected - consumer disconnected`)
    })
    // this._consumer.on('event.log', (log) => {
    //   // Logger.silly(`consumer log: ${log.fac} - ${log.message}`);
    // })
    this._consumer.on('event.log', log => {
      // Logger.silly(`Consumer[${this._topics}].on_event_log - ${log.message}`)
    })
  }

  /**
   * Connect consumer
   *
   * @fires Consumer#ready
   *
   * Connects consumer to the Kafka brocker, and sets up the configured processing mode
   * @return {Promise} - Returns a promise: resolved if successful, or rejection if connection failed
   */
  connect () {
    this._consumer.connect()
  }

  /**
   * Disconnect consumer
   *
   * Disconnects consumer from the Kafka brocker
   */
  disconnect (cb = () => {}) {
    this._consumer.unsubscribe()

    this._consumer.disconnect(() => {
      // disconnected
      cb()
    })
  }

  // _on_rebalancing_cb (err, assignment) {
  //   Logger.debug(`Consumer[${this._topics}].on_rebalancing_cb - assignment: ${assignment}`)
  //   if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
  //     // Note: this can throw when you are disconnected. Take care and wrap it in
  //     // a try catch if that matters to you
  //     // this.assign(assignment);
  //   } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
  //     // Same as above
  //     // this.unassign();
  //   } else {
  //     // We had a real error
  //     Logger.error(`Consumer[${this._topics}].on_rebalancing_cb - error: ${err}`)
  //   }
  // }

  _on_subscribed (topics) {
    Logger.debug(`Consumer[${this._topics}].on_subscribed - topics ${topics}`)

    // TODO: check if the topics we requested are in this array

    // consumer.consume(); // flowing mode / stream

    this._consumer.consume()

    // setInterval(()=>{
    // 	this._consumer.consume(1)
    // }, 1000);
    this.emit('ready')
  }

  // _offset_commit_cb (err, topic_partitions) {
  //   if (err) {
  //     // There was an error committing
  //     Logger.error(`Consumer[${this._topics}].on_commit_cb - error: ${err}`)
  //   } else {
  //     // Commit went through. Let's log the topic partitions
  //     Logger.debug(`Consumer[${this._topics}].on_commit_cb - Commit ok for topic: ${topic_partitions}`)
  //   }
  //
  //   this._consumer.consume() //
  // }

  async _on_data (data) {
    Logger.debug(`Consumer[${this._topics}].on_data - topic: ${data.topic} partition: ${data.partition} offset: ${data.offset}`)

    // const msg = JSON.parse(data.value.toString())
    const msg = {
      key: data.key,
      offset: data.offset,
      partition: data.partition,
      size: data.size,
      timestamp: data.timestamp,
      topic: data.topic,
      value: Protocol.parseValue(data.value, 'utf8', true)
    }


    // if (!msg || !msg.timestamp) { return }
    // call handler
    // varl err = ''
    // this._handler_fn(null, msg, async (err) => {
    //   // if(err) // what to do with returned errors?
    //   Logger.silly(`Consumer[${data.topic}].cb()`)
    //   // done, get
    //   // this._consumer.commitMessage(data)
    //   this._consumer.consume(1); // uncomment if the using auto-commit=true
    // })

    await this._handler_fn(null, msg)
    // this._consumer.consume(1)
  }

  _on_error (err) {
    Logger.error(err)
  }

  /**
   * This callback returns the message read from Kafka.
   *
   * @callback Consumer~workDoneCb
   * @param {Error} error - An error, if one occurred while reading
   * the data.
   * @param {object} messages - Either a list or a single message @see KafkaConsumer~Message
   * @returns {Promise} - Returns resolved on success, or rejections on failure
   */

  /**
   * Consume
   *
   * Consume messages from Kafka as per the configuration specified in the constructor.
   * @param {Consumer~workDoneCb} workDoneCb - Callback function to process the consumed message
   */
  consume (workDoneCb) {}

  /**
   * Commit message in sync mode
   *
   * @param {KafkaConsumer~Message} msg - Kafka message to be commited
   */
  commitMessageSync (msg) {}
}

module.exports = Consumer
