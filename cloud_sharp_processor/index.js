const fs = require('fs-extra')
const path = require('path')
const { processFile } = require('gatsby-plugin-sharp/process-file')
const {PubSub} = require('@google-cloud/pubsub');
const {Storage} = require('@google-cloud/storage');

const MAX_PUBSUB_RESPONSE_SIZE = 1024 * 1024 // 1mb

const pubSubClient = new PubSub();
const storageClient = new Storage();

process.chdir('/tmp')

async function processPubSubMessageOrStorageObject(msg) {
  let data = null

  console.log(`Got msg: ${JSON.stringify(msg)}`)

  if (msg.bucket && msg.name) {
    const bucket = storageClient.bucket(msg.bucket)
    const file = bucket.file(msg.name)
    await file.download({destination: `/tmp/${msg.name}`})
    data = (await fs.readFile(`/tmp/${msg.name}`)).toString()
  } else {
    data = msg.data
  }

  return JSON.parse(Buffer.from(data, 'base64').toString());
}

exports.gatsbySharpProcessor = async (msg, context) => {
  const event = await processPubSubMessageOrStorageObject(msg)
  try {
    const file = Buffer.from(event.file)
    const results = processFile(file, event.action.operations, event.action.pluginOptions)
    const tranforms = await Promise.all(results)

    const result = {
      type: 'JOB_COMPLETED',
      payload: {
        id: event.id,
      }
    }

    let size = 0
    const output = []
    await Promise.all(tranforms.map(async t => {
      const data = await fs.readFile(t.outputPath)
      size += data.length
      output.push({...t, data: data.toString('base64')})
    }))

    if (size > MAX_PUBSUB_RESPONSE_SIZE) {
      const storageResult = {
        id: event.id,
        output
      }
      await storageClient.bucket(`event-results-${event.topic}`).file(`result-${event.id}`).save(Buffer.from(JSON.stringify(storageResult)))
      result.payload.storedResult = `result-${event.id}`
    } else {
      result.payload.output = output
    }

    const resultMsg = Buffer.from(JSON.stringify(result))
    const messageId = await pubSubClient.topic(event.topic).publish(resultMsg)
    console.log("Published message ", messageId, resultMsg.length, result.payload.storedResult)
    await fs.emptyDir('/tmp')
  } catch (err) {
    const messageId = await pubSubClient.topic(event.topic).publish(Buffer.from(JSON.stringify({
      type: 'JOB_FAILED',
      payload: {
        id: event.id,
        error: err.toString()
      }
    })))
    console.error("Failed to process message:", messageId, err)
    await fs.emptyDir('/tmp')
  }
};
