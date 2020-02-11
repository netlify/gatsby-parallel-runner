#!/usr/bin/env node

const cp = require('child_process')
const path = require('path')
const fs = require('fs-extra')
const log = require('loglevel')
const { PubSub } = require('@google-cloud/pubsub')
const { Storage } = require('@google-cloud/storage')

exports.build = function() {
  log.setLevel(process.env.PARALLEL_RUNNER_LOG_LEVEL || 'warn')

  const MESSAGE_TYPES = {
    LOG_ACTION: `LOG_ACTION`,
    JOB_CREATED: `JOB_CREATED`,
    JOB_COMPLETED: `JOB_COMPLETED`,
    JOB_FAILED: `JOB_FAILED`,
    ACTIVITY_START: `ACTIVITY_START`,
    ACTIVITY_END: `ACTIVITY_END`,
    ACTIVITY_SUCCESS: `ACTIVITY_SUCCESS`,
    ACTIVITY_ERROR: `ACTIVITY_ERROR`
  }

  const JOB_TYPES = {
    IMAGE_PROCESSING: processImage
  }

  // default 5 minute timeout
  const MAX_JOB_TIME = process.env.PARALLEL_RUNNER_TIMEOUT ? parseInt(process.env.PARALLEL_RUNNER_TIMEOUT, 10) : 5 * 60 * 1000
  const MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte
  const MAX_MEM_MESSAGE_MEM = 1024 * 1024 * 5 * 10 // 500 megabytes

  process.env.ENABLE_GATSBY_EXTERNAL_JOBS = true

  const jobsInProcess = new Map()
  const gatsbyProcess = cp.fork(`${process.cwd()}/node_modules/.bin/gatsby`, ['build']);

  let messageMemUsage = 0

  const config = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))
  const pubSubClient = new PubSub({
    projectId: config.project_id
  });
  const storage = new Storage({
    projectId: config.project_id
  });

  const subName = `nf-sub-${new Date().getTime()}`
  const bucketName = `event-processing-${process.env.WORKER_TOPIC}`
  const resultBucketName = `event-results-${process.env.TOPIC}`

  function failJob(id, error) {
    jobsInProcess.delete(id)
    gatsbyProcess.send({
      type: MESSAGE_TYPES.JOB_FAILED,
      payload: {
        id, error
      }
    })
  }

  function pubsubMessageHandler(msg) {
    msg.ack()
    const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
    if (log.getLevel() <= log.levels.TRACE) {
      log.trace("Got worker message", msg.id, pubSubMessage.type, pubSubMessage.payload && pubSubMessage.payload.id)
    }

    switch (pubSubMessage.type) {
      case MESSAGE_TYPES.JOB_COMPLETED:
        if (jobsInProcess.has(pubSubMessage.payload.id)) {
          const callback = jobsInProcess.get(pubSubMessage.payload.id)
          callback(pubSubMessage.payload)
        }
        return
      case MESSAGE_TYPES.JOB_FAILED:
        if (jobsInProcess.has(pubSubMessage.payload.id)) {
          failJob(pubSubMessage.payload.id, pubSubMessage.payload.error)
        }
        return
      default:
        log.error("Unkown worker message: ", msg)
    }
  }

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

  async function pushToQueue(msg, file) {
    const stat = await fs.stat(file)
    await waitForFreeMessageMem()
    messageMemUsage += stat.size
    const data = await fs.readFile(file)
    const pubsubMsg = Buffer.from(JSON.stringify({
      file: data,
      action: msg.args,
      topic: process.env.TOPIC,
      id: msg.id
    }))
    if (pubsubMsg.length < MAX_PUB_SUB_SIZE) {
      log.debug("Publishing to message queue", file, msg.id)
      await pubSubClient.topic(process.env.WORKER_TOPIC).publish(pubsubMsg);
    } else {
      log.debug("Publishing to storage queue", file, msg.id)
      await storage.bucket(bucketName).file(`event-${msg.id}`).save(pubsubMsg.toString('base64'));
    }
    messageMemUsage -= stat.size
  }

  function finalizeJobHandler(msgId, file, outputDir) {
    return async (result) => {
      log.trace("Finalizing for", file, msgId)
      jobsInProcess.delete(msgId)
      try {
        let output = null
        if (result.storedResult) {
          const bucket = storage.bucket(resultBucketName)
          const file = bucket.file(result.storedResult)
          await file.download({destination: `/tmp/result-${msgId}`})
          const data = (await fs.readFile(`/tmp/result-${msgId}`)).toString()
          output = JSON.parse(data).output
          await fs.remove(`/tmp/result-${msgId}`)
        } else {
          output = result.output
        }

        await Promise.all(output.map(async (transform) => {
          const filePath = path.join(outputDir, transform.outputPath)
          await fs.mkdirp(path.dirname(filePath))
          return fs.writeFile(filePath, Buffer.from(transform.data, 'base64'))
        }))
        gatsbyProcess.send({
          type: MESSAGE_TYPES.JOB_COMPLETED,
          payload: {
            id: msgId,
            result: {output: output.map(t => ({outputPath: t.outputPath, args: t.args}))}
          }
        })
      } catch (err) {
        log.error("Failed to execute callback", err)
        failJob(msgId, `File failed to process result from ${file}: ${err}`)
      }
    }
  }

  function timeoutHandler(msgId, file) {
    return function() {
      log.trace("Checking timeout for", file, msgId)
      if (jobsInProcess.has(msgId)) {
        log.error("Timing out job for file", file, msgId)
        failJob(msgId, `File failed to process with timeout ${file}`)
      }
    }
  }

  async function createSubscription() {
    // Creates a new subscription
    try {
      await pubSubClient.createTopic(process.env.TOPIC)
    } catch(err) {
      log.trace("Create topic failed", err)
    }

    const [subscription] = await pubSubClient.topic(process.env.TOPIC).createSubscription(subName)
    log.debug("Got subscription: ", subscription)

    subscription.on('message', pubsubMessageHandler)
    subscription.on('error', (err) => log.error("Error from subscription: ", err))
    subscription.on('close', (err) => log.error("Subscription closed unexpectedly", err))

    gatsbyProcess.on('exit', async (code) => {
      log.debug("Removing listener")
      subscription.removeListener('message', pubsubMessageHandler)
      process.exit(code)
    });
  }

  createSubscription().catch(log.error);

  gatsbyProcess.on('message', (msg) => {
    if (log.getLevel() <= log.levels.TRACE && msg.type !== MESSAGE_TYPES.LOG_ACTION) {
      log.trace("Got gatsby message", JSON.stringify(msg))
    }
    switch (msg.type) {
      case MESSAGE_TYPES.JOB_CREATED: {
        if (JOB_TYPES[msg.payload.name]) {
          JOB_TYPES[msg.payload.name](msg.payload)
        } else {
          gatsbyProcess.send({
            type: JOB_NOT_WHITELISTED,
            payload: {
              id: msg.payload.id
            }
          })
        }
        break
      }
      case MESSAGE_TYPES.LOG_ACTION:
        // msg.action.payload.text && console.log(msg.action.payload.text)
        break
      default:
        log.warn("Ignoring message: ", msg)
    }
  });

  async function processImage(msg) {
    if (!msg.inputPaths || msg.inputPaths.length > 1) {
      log.error("Wrong number of input paths in msg: ", msg)
      failJob(msg.id, 'Wrong number of input paths')
      return
    }

    const file = msg.inputPaths[0].path

    jobsInProcess.set(msg.id, finalizeJobHandler(msg.id, file, msg.outputDir))
    try {
      await pushToQueue(msg, file)
      setTimeout(timeoutHandler(msg.id, file), MAX_JOB_TIME)
    } catch(err) {
      log.error("Error during publish: ", err)
      failJob(msg.id, `Failed to publish job ${err}`)
    }
  }
}
