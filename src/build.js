#!/usr/bin/env node

const cp = require('child_process')
const path = require('path')
const fs = require('fs-extra')
const { PubSub } = require('@google-cloud/pubsub')
const { Storage } = require('@google-cloud/storage')

exports.build = function() {
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

  const MAX_JOB_TIME = 60 * 1000 // 60 seconds timeout
  const MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte

  process.env.ENABLE_GATSBY_EXTERNAL_JOBS = true

  const jobsInProcess = new Map()
  const gatsbyProcess = cp.fork(`${process.cwd()}/node_modules/.bin/gatsby`, ['build']);

  const config = JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))
  const pubSubClient = new PubSub({
    projectId: config.project_id
  });
  const storage = new Storage({
    projectId: config.project_id
  });


  const subName = `nf-sub-${new Date().getTime()}`
  const bucketName = `event-processing-${process.env.WORKER_TOPIC}`

  function pubsubMessageHandler(msg) {
    msg.ack()
    const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
    switch (pubSubMessage.type) {
      case MESSAGE_TYPES.JOB_COMPLETED:
        if (jobsInProcess.has(pubSubMessage.payload.id)) {
          const callback = jobsInProcess.get(pubSubMessage.payload.id)
          callback(pubSubMessage.payload)
        }
        return
      case MESSAGE_TYPES.JOB_FAILED:
        if (jobsInProcess.has(pubSubMessage.payload.id)) {
          jobsInProcess.delete(pubSubMessage.payload.id)
          gatsbyProcess.send({
            type: MESSAGE_TYPES.JOB_FAILED,
            payload: pubSubMessage.payload
          })
        }
        return
      default:
        console.error("Unkown worker message: ", msg)
    }

  }

  async function createSubscription() {
    // Creates a new subscription
    try {
      await pubSubClient.createTopic(process.env.TOPIC)
    } catch(err) {
      // console.log("Create topic failed", err)
    }

    const [subscription] = await pubSubClient.topic(process.env.TOPIC).createSubscription(subName);

    subscription.on('message', pubsubMessageHandler);

    gatsbyProcess.on('exit', async (code) => {
      subscription.removeListener('message', pubsubMessageHandler);
      process.exit(code)
    });
  }

  createSubscription().catch(console.error);

  gatsbyProcess.on('message', (msg) => {
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
        msg.action.payload.text && console.log(msg.action.payload.text)
        break
      default:
        console.log("Ignoring message: ", msg)
    }
  });

  async function processImage(msg) {
    if (!msg.inputPaths || msg.inputPaths.length > 1) {
      console.error("Wrong number of input paths in msg: ", msg)
      gatsbyProcess.send({
        type: MESSAGE_TYPES.JOB_FAILED,
        payload: {
          id: msg.id,
          error: 'Wrong number of input paths'
        }
      })
      return
    }

    const file = msg.inputPaths[0].path
    const data = await fs.readFile(file)
    jobsInProcess.set(msg.id, async (result) => {
      try {
        await Promise.all(result.output.map(async (transform) => {
          const filePath = path.join(msg.outputDir, transform.outputPath)
          await fs.mkdirp(path.dirname(filePath))
          return fs.writeFile(filePath, Buffer.from(transform.data, 'base64'))
        }))
        gatsbyProcess.send({
          type: MESSAGE_TYPES.JOB_COMPLETED,
          payload: {
            id: msg.id,
            result: {output: result.output.map(t => ({outputPath: t.outputPath, args: t.args}))}
          }
        })
        jobsInProcess.delete(msg.id)
      } catch (err) {
        console.error("Failed to execute callback", err)
      }
    })
    try {
      const pubsubMsg = Buffer.from(JSON.stringify({
        file: data,
        action: msg.args,
        topic: process.env.TOPIC,
        id: msg.id
      }))
      if (pubsubMsg.length < MAX_PUB_SUB_SIZE) {
        await pubSubClient.topic(process.env.WORKER_TOPIC).publish(pubsubMsg);
      } else {
        await storage.bucket(bucketName).file(`event-${msg.id}`).save(pubsubMsg.toString('base64'));
      }

      setTimeout(() => {
        if (jobsInProcess.has(msg.id)) {
          console.log("Timing out job for file", file)
          jobsInProcess.delete(msg.id)
          gatsbyProcess.send({
            type: MESSAGE_TYPES.JOB_FAILED,
            payload: {
              id: msg.id,
              error: `File failed to process with timeout ${file}`
            }
          })
        }
      }, MAX_JOB_TIME)
    } catch(err) {
      console.error("Error during publish: ", err)
    }
  }
}
