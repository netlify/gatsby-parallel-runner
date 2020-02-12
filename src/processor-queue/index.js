
const fs = require('fs-extra')
const log = require('loglevel')
const { PubSub } = require('@google-cloud/pubsub')
const { Storage } = require('@google-cloud/storage')

const MAX_JOB_TIME = process.env.PARALLEL_RUNNER_TIMEOUT ? parseInt(process.env.PARALLEL_RUNNER_TIMEOUT, 10) : 5 * 60 * 1000
const MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte
const MAX_MEM_MESSAGE_MEM = 1024 * 1024 * 5 * 10 // 500 megabytes

const MESSAGE_TYPES = {
  JOB_COMPLETED: `JOB_COMPLETED`,
  JOB_FAILED: `JOB_FAILED`
}

let messageMemUsage = 0
let initialized = false
const jobsInProcess = new Map()

function hasFreeMessageMem() {
  return messageMemUsage < MAX_MEM_MESSAGE_MEM
}

async function waitForFreeMessageMem() {
  return new Promise((resolve, reject) => {
    const check = () => {
      if (hasFreeMessageMem()) { return resolve() }
      setTimeout(check, 200)
    }
    check()
  })
}

function finalizeJobHandler(id, resolve, reject) {
  return async (result) => {
    log.trace("Finalizing for", id)
    jobsInProcess.delete(id)
    try {
      let output = null
      if (result.storedResult) {
        const resultBucketName = `event-results-${process.env.TOPIC}`
        const bucket = storage.bucket(resultBucketName)
        const file = bucket.file(result.storedResult)
        await file.download({destination: `/tmp/result-${msgId}`})
        const data = (await fs.readFile(`/tmp/result-${msgId}`)).toString()
        output = JSON.parse(data).output
        await fs.remove(`/tmp/result-${msgId}`)
      } else {
        output = result.output
      }

      resolve(output)
    } catch (err) {
      log.error("Failed to execute callback", err)
      reject(`Failed to process result from job ${id}: ${err}`)
    }
  }
}

function timeoutHandler(id, callback) {
  return function() {
    log.trace("Checking timeout for", file, msgId)
    if (jobsInProcess.has(msgId)) {
      log.error("Timing out job for file", file, msgId)
      callback(`Timeout for job with id: ${id}`)
    }
  }
}

function pubsubMessageHandler(msg) {
  msg.ack()
  const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
  log.debug("Got worker message", msg.id, pubSubMessage.type, pubSubMessage.payload && pubSubMessage.payload.id)

  switch (pubSubMessage.type) {
    case MESSAGE_TYPES.JOB_COMPLETED:
      if (jobsInProcess.has(pubSubMessage.payload.id)) {
        const job = jobsInProcess.get(pubSubMessage.payload.id)
        job.resolve(pubSubMessage.payload)
      }
      return
    case MESSAGE_TYPES.JOB_FAILED:
      if (jobsInProcess.has(pubSubMessage.payload.id)) {
        const job = jobsInProcess.get(pubSubMessage.payload.id)
        job.reject(pubSubMessage.payload.error)
      }
      return
    default:
      log.error("Unkown worker message: ", msg)
  }
}

exports.initialize = async function() {
  const config = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))
  const pubSubClient = new PubSub({projectId: config.project_id})
  const storageClient = new Storage({projectId: config.project_id})

  const subName = `nf-sub-${new Date().getTime()}`

  async function createSubscription() {
    // Creates a new subscription
    try {
      await pubSubClient.createTopic(process.env.TOPIC)
    } catch(err) {
      log.trace("Create topic failed", err)
    }

    const [subscription] = await pubSubClient.topic(process.env.TOPIC).createSubscription(subName)
    log.trace("Got subscription: ", subscription)

    subscription.on('message', pubsubMessageHandler)
    subscription.on('error', (err) => log.error("Error from subscription: ", err))
    subscription.on('close', (err) => log.error("Subscription closed unexpectedly", err))
  }

  await createSubscription()

  initialized = true
  return {
    process: (payload) => runTask(pubSubClient, storageClient, payload),
    stop: () => {
      subscription.removeListener('message', pubsubMessageHandler)
    }
  }
}

async function runTask(pubSubClient, storageClient, payload) {
  log.debug("runTaks")
  if (!initialized) {
    log.error("Not initialized")
    throw("Queue has not been initialized")
  }

  return new Promise(async (resolve, reject) => {
    log.debug("Waiting for free memory")
    await waitForFreeMessageMem()
    try {
      const {id, args, file} = payload
      log.debug("Setting up job", id)
      jobsInProcess.set(id, finalizeJobHandler(id, {resolve, reject}))
      let size = 0
      let data = null
      if (file instanceof Buffer) {
        size = file.byteLength
        messageMemUsage += size
      } else {
        size = (await fs.stat(file)).size
        messageMemUsage += size
      }
      // Note, if we moved the reading into the first if block we would create a race condition
      // where we don't increase mem usage before we've finished reading the data
      if (file instanceof Buffer) {
        data = file.toString()
      } else {
        data = await fs.readFile(file)
      }
      log.debug("Generating msg for", id)
      const pubsubMsg = Buffer.from(JSON.stringify({ id, file: data, action: args, topic: process.env.TOPIC }))
      if (pubsubMsg.length < MAX_PUB_SUB_SIZE) {
        log.debug("Publishing to message queue", id)
        await pubSubClient.topic(process.env.WORKER_TOPIC).publish(pubsubMsg);
      } else {
        log.debug("Publishing to storage queue", id)
        const bucketName = `event-processing-${process.env.WORKER_TOPIC}`
        await storageClient.bucket(bucketName).file(`event-${id}`).save(pubsubMsg.toString('base64'));
      }
      messageMemUsage -= stat.size
      log.debug("Message sent")
      setTimeout(timeoutHandler(id, reject), MAX_JOB_TIME)
    } catch(error) {
      log.error("Error publishing to queue", error)
      reject(error)
    }
  })
}