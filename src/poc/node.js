
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
const Kafka = require('kafka-node')
const assert = require('assert')
const EventEmitter = require('events')
const Protocol = require('../kafka/protocol')
const async = require('async')

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
    this._initialised = false
    this._syncEnabled = config.options.sync

    Logger.debug(`Consumer[${this._topics}]._broker_list: ${this._broker_list}`)
    Logger.debug(`Consumer[${this._topics}]._group_id: ${this._group_id}`)
    Logger.debug(`Consumer[${this._topics}]._client_id: ${this._client_id}`)
    Logger.debug(`Consumer[${this._topics}]._topics: ${this._topics}`)
    Logger.debug(`Consumer[${this._topics}]._syncEnabled: ${this._syncEnabled}`)


    // const consumer_id = Math.random().toString(36).substring(2, 6) + Math.random().toString(36).substring(2, 6);

    let consumer_group_options = {
      kafkaHost: this._broker_list,
      id: this._client_id,
      groupId: this._group_id,
      // sessionTimeout: 15000,
      // An array of partition assignment protocols ordered by preference.
      // 'roundrobin' or 'range' string for built ins (see below to pass in custom assignment protocol)
      // protocol: ['roundrobin'],
      autoCommit: false,
      fromOffset: 'latest', // default is latest
      connectOnReady: true,
      onRebalance: this._on_rebalancing_cb.bind(this)
    }

    if (this._syncEnabled === true) {
      Logger.debug(`Consumer[${this._topics}].constructor - creating sync queue`)
      this._syncQueue = async.queue( async (message) => {
        await this._handler_fn(null, message)
        Logger.debug(`NodeConsumer[${this._topics}]._commit`)
        this._commit(message)
      })
    }

    this._consumer = new Kafka.ConsumerGroup(consumer_group_options, this._topics)

    this._consumer.on('connect', () => {
      if (!this._initialised) {
        this._initialised = true
        this.emit('ready')
      }

      // else -> reconnected
    })

    // important events
    // this._consumer.on("subscribed", this._on_subscribed.bind(this));
    this._consumer.on('message', this._on_data.bind(this))
    // this._consumer.on('event.error', this._on_error.bind(this));

    // other events
    this._consumer.on('ready', () => {
      Logger.info(`NodeConsumer[${this._topics}].on_ready - connected`)
    })

    this._consumer.on('done', (topic) => {
      Logger.silly(`NodeConsumer[${this._topics}].on_done - consumer done`)
    })

    // this._consumer.on("disconnected", ()=>{
    // 	console.log(`consumer disconnected`);
    // });
    // this._consumer.on("event.log", (log)=>{
    // 	// console.log(`consumer log: ${log.fac} - ${log.message}`);
    // });
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
    // this._consumer.unsubscribe();

    this._consumer.disconnect(() => {
      // disconnected
      cb()
    })
  }

  _on_rebalancing_cb (isAlreadyMember, callback) {
    if (isAlreadyMember) {
      // commit before rebalancing if already part of a group
      return this._consumer.commit(callback)
    }

    callback()
  }

  _commit (message) {
    // done, get
    // if(data.partition === 4)
    this._consumer.commit(true, (err, commit_res) => {
      this._topics.forEach((topic) => {
        if (commit_res.hasOwnProperty(topic) && commit_res[topic].hasOwnProperty('partition')) {
          if (Object.keys(commit_res[topic]).length <= 0) { return }

          const per_topic_commit_res = commit_res[topic]
          if (per_topic_commit_res['errorCode']) { console.warn(`commited partition ${per_topic_commit_res['partition']} with errorCode: ${per_topic_commit_res['partition']}`) } else { console.log(`commited partition ${per_topic_commit_res['partition']}`) }
        }
      })

      // if(commit_res[Object.keys(commit_res)[0]].hasOwnProperty("partition")){
      //
      // }
    })
    // this._consumer.consume(1);
  }

  async _on_data (data) {
    Logger.debug(`NodeConsumer[${this._topics}].on_data - topic: ${data.topic} partition: ${data.partition} offset: ${data.offset}`)
    // this._consumer.pause();

    const msg = {
      key: data.key,
      offset: data.offset,
      partition: data.partition,
      size: data.size,
      timestamp: data.timestamp,
      topic: data.topic,
      value: Protocol.parseValue(data.value, 'utf8', true)
    }

    // call handler
    // await this._handler_fn(msg, (err) => {
    //   // if(err) // what to do with returned errors?
    //   Logger.debug(`NodeConsumer[${this._topics}]._commit`)
    //   this._commit(msg)
    // })

    if (this._syncEnabled) {
      this._syncQueue.push(msg)
    } else {
      await this._handler_fn(null, msg)
      Logger.debug(`NodeConsumer[${this._topics}]._commit`)
      this._commit(msg)
    }
  }

  _on_error (err) {
    console.error(err)
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
