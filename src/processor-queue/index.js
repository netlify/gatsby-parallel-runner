'use strict';

const fs = require('fs-extra')
const log = require('loglevel')
const { PubSub } = require('@google-cloud/pubsub')
const { Storage } = require('@google-cloud/storage')

const DEFAULT_MAX_JOB_TIME = process.env.PARALLEL_RUNNER_TIMEOUT ? parseInt(process.env.PARALLEL_RUNNER_TIMEOUT, 10) : 5 * 60 * 1000
const DEFAULT_MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte
const DEFAULT_MAX_MESSAGE_MEM = 1024 * 1024 * 5 * 10 // 500 megabytes

const MESSAGE_TYPES = {
  JOB_COMPLETED: `JOB_COMPLETED`,
  JOB_FAILED: `JOB_FAILED`
}

class GooglePubSub {
  constructor({maxPubSubSize, noSubscription}) {
    this.maxPubSubSize = maxPubSubSize || DEFAULT_MAX_PUB_SUB_SIZE
    const config = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))
    this.subName = `nf-sub-${new Date().getTime()}`
    this.bucketName = `event-processing-${process.env.WORKER_TOPIC}`
    this.resultBucketName = `event-results-${process.env.TOPIC}`
    this.pubSubClient = new PubSub({projectId: config.project_id});
    this.storageClient = new Storage({projectId: config.project_id});
    this.subscribers = []

    return (async () => {
      try {
        if (!noSubscription) {
          await this._createSubscription()
        }
      } catch (err) {
        return Promise.reject(`Failed to start Google PubSub subscriptionn: ${err}`)
      }

      return this;
    })();
  }

  subscribe(handler) {
    this.subscribers.push(handler)
  }

  async publish(id, msg) {
    if (msg.byteLength < this.maxPubSubSize) {
      log.debug(`Publishing ${id} to pubsub`)
      await this.pubSubClient.topic(process.env.WORKER_TOPIC).publish(msg);
    } else {
      log.debug(`Publishing ${id} to storage`)
      await this.storageClient.bucket(this.bucketName).file(`event-${id}`).save(msg.toString('base64'), {resumable: false});
    }
  }

  async _messageHandler(msg) {
    msg.ack()
    const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
    const {payload} = pubSubMessage
    if (payload && payload.storedResult) {
      payload.output = await this._downloadFromStorage(msg.id, payload.storedResult)
    }

    this.subscribers.forEach(handler => handler(pubSubMessage))
  }

  async _createSubscription() {
    // Creates a new subscription
    try {
      await this.pubSubClient.createTopic(process.env.TOPIC)
    } catch(err) {
      log.trace("Create topic failed", err)
    }

    const [subscription] = await this.pubSubClient.topic(process.env.TOPIC).createSubscription(this.subName)

    subscription.on('message', this._messageHandler.bind(this))
    subscription.on('error', (err) => log.error("Error from subscription: ", err))
    subscription.on('close', (err) => log.error("Subscription closed unexpectedly", err))
  }

  async _downloadromStorage(id, storedResult) {
    const file = this.storageClient.bucket(this.resultBucketName).file(storedResult)
    await file.download({destination: `/tmp/result-${msg.id}`})
    const data = (await fs.readFile(`/tmp/result-${msg.id}`)).toString()
    const output = JSON.parse(data).output
    await fs.remove(`/tmp/result-${msgId}`)
    return output
  }
}

class Job {
  constructor({id, args, file}) {
    this.id = id
    this.args = args
    this.file = file

    return (async () => {
      try {
        await this._calculateSize()
      } catch (err) {
        return Promise.reject(err)
      }
      return this
    })();
  }

  async msg() {
    const data = await this._readData()
    return Buffer.from(JSON.stringify({ id: this.id, file: data.toString('base64'), action: this.args, topic: process.env.TOPIC }))
  }

  async _calculateSize() {
    if (this.file instanceof Buffer) {
      return this.fileSize = this.file.byteLength
    }
    try {
      const stat = await fs.stat(this.file)
      this.fileSize = stat.size
    } catch(err) {
      return Promise.reject(err)
    }
  }

  async _readData() {
    if (this.file instanceof Buffer) {
      return this.file
    }
    return await fs.readFile(this.file)
  }
}

class Queue {
  constructor({maxJobTime, pubSubImplementation}) {
    this._jobs = new Map()
    this.maxJobTime = maxJobTime || DEFAULT_MAX_JOB_TIME
    this.pubSubImplementation = pubSubImplementation
    pubSubImplementation && pubSubImplementation.subscribe(this._onMessage.bind(this))
  }

  async push(id, msg) {
    return new Promise(async (resolve, reject) => {
      this._jobs.set(id, {resolve, reject})
      setTimeout(() => {
        if (this._jobs.has(id)) {
          reject(`Job timed out ${id}`)
        }
      }, this.maxJobTime)
      try {
        await this.pubSubImplementation.publish(id, msg)
      } catch(err) {
        reject(err)
      }
    })
  }

  _onMessage(pubSubMessage) {
    const {type, payload} = pubSubMessage
    log.debug("Got worker message", type, payload && payload.id)

    switch (type) {
      case MESSAGE_TYPES.JOB_COMPLETED:
        if (this._jobs.has(payload.id)) {
          this._jobs.get(payload.id).resolve(payload)
          this._jobs.delete(payload.id)
        }
        return
      case MESSAGE_TYPES.JOB_FAILED:
        if (this._jobs.has(payload.id)) {
          this._jobs.get(payload.id).reject(payload.error)
          this._jobs.delete(payload.id)
        }
        return
      default:
        log.error("Unkown worker message: ", pubSubMessage)
    }
  }
}

class Processor {
  constructor({maxJobTime, maxMessageMem, pubSubImplementation}) {
    this._mem = 0
    this.maxMessageMem = maxMessageMem || DEFAULT_MAX_MESSAGE_MEM

    this.queue = new Queue({maxJobTime, maxMessageMem, pubSubImplementation})
  }

  async process(payload) {
    let size = 0
    try {
      const job = await new Job(payload)
      size = job.fileSize
      await this._waitForFreeMessageMem()
      this._mem += size
      const msg = await job.msg()
      const result = await this.queue.push(job.id, msg)
      this._mem -= size
      return result
    } catch (err) {
      this._mem -= size
      return Promise.reject(err)
    }
  }

  async _waitForFreeMessageMem() {
    return new Promise((resolve, reject) => {
      const check = () => {
        if (this._mem <= this.maxMessageMem) { return resolve() }
        setTimeout(check, 100)
      }
      check()
    })
  }
}

exports.Processor = Processor
exports.GooglePubSub = GooglePubSub
exports.Job = Job