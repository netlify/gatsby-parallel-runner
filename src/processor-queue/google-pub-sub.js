const { PubSub } = require("@google-cloud/pubsub")
const { Storage } = require("@google-cloud/storage")
const fs = require("fs-extra")
const path = require("path")
const log = require("loglevel")

const DEFAULT_MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte

class GooglePubSub {
  constructor({ maxPubSubSize, noSubscription }) {
    this.maxPubSubSize = maxPubSubSize || DEFAULT_MAX_PUB_SUB_SIZE
    const config = JSON.parse(
      fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS)
    )
    this.subName = `nf-sub-${new Date().getTime()}`
    this.bucketName = `event-processing-${process.env.WORKER_TOPIC}`
    this.resultBucketName = `event-results-${process.env.TOPIC}`
    this.pubSubClient = new PubSub({ projectId: config.project_id })
    this.storageClient = new Storage({ projectId: config.project_id })
    this.subscribers = []

    return (async () => {
      const topicCreatedFile = path.join(
        ".cache",
        `topic-created-${process.env.TOPIC}`
      )
      const exists = await fs.pathExists(topicCreatedFile)
      if (exists) {
        return this
      }

      try {
        if (!noSubscription) {
          await this._createSubscription()
        }
      } catch (err) {
        return Promise.reject(
          `Failed to start Google PubSub subscriptionn: ${err}`
        )
      }

      try {
        const lifeCycle = `<?xml version="1.0" ?>
        <LifecycleConfiguration>
            <Rule>
                <Action>
                    <Delete/>
                </Action>
                <Condition>
                    <Age>30</Age>
                </Condition>
            </Rule>
        </LifecycleConfiguration>`
        const [bucket] = await this.storageClient.createBucket(
          this.resultBucketName
        )
        await bucket.setMetadata({ lifeCycle })
      } catch (err) {
        console.log("Create result bucket failed", err)
      }

      await fs.ensureFile(topicCreatedFile)

      return this
    })()
  }

  subscribe(handler) {
    this.subscribers.push(handler)
  }

  async publish(id, msg) {
    if (msg.byteLength < this.maxPubSubSize) {
      log.debug(`Publishing ${id} to pubsub`)
      await this.pubSubClient.topic(process.env.WORKER_TOPIC).publish(msg)
    } else {
      log.debug(`Publishing ${id} to storage`)
      await this.storageClient
        .bucket(this.bucketName)
        .file(`event-${id}`)
        .save(msg.toString("base64"), { resumable: false })
    }
  }

  async _messageHandler(msg) {
    msg.ack()
    const pubSubMessage = JSON.parse(Buffer.from(msg.data, "base64").toString())
    const { payload } = pubSubMessage
    if (payload && payload.storedResult) {
      payload.output = await this._downloadFromStorage(
        msg.id,
        payload.storedResult
      )
    }

    this.subscribers.forEach(handler => handler(pubSubMessage))
  }

  async _createSubscription() {
    // Creates a new subscription
    try {
      await this.pubSubClient.createTopic(process.env.TOPIC)
    } catch (err) {
      log.trace("Create topic failed", err)
    }

    const [subscription] = await this.pubSubClient
      .topic(process.env.TOPIC)
      .createSubscription(this.subName)

    subscription.on("message", this._messageHandler.bind(this))
    subscription.on("error", err => log.error("Error from subscription: ", err))
    subscription.on("close", err =>
      log.error("Subscription closed unexpectedly", err)
    )
  }

  async _downloadFromStorage(id, storedResult) {
    const file = this.storageClient
      .bucket(this.resultBucketName)
      .file(storedResult)
    await file.download({ destination: `/tmp/result-${id}` })
    const data = (await fs.readFile(`/tmp/result-${id}`)).toString()
    const output = JSON.parse(data).output
    await fs.remove(`/tmp/result-${id}`)
    return output
  }
}

exports.GooglePubSub = GooglePubSub
