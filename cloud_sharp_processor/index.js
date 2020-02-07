const fs = require('fs-extra')
const path = require('path')
const { processFile } = require('gatsby-plugin-sharp/process-file')
const {PubSub} = require('@google-cloud/pubsub');

const pubSubClient = new PubSub();

process.chdir('/tmp')

async function processPubSubMessageOrStorageObject(msg) {
  let data = null

  if (msg.download) {
    data = await msg.download()
  } else {
    data = msg.data
  }

  return JSON.parse(Buffer.from(data, 'base64').toString());
}

exports.gatsbySharpProcessor = async (msg, context) => {
  const event = await processPubSubMessageOrStorageObject(msg)
  try {
    const file = event.file
    const results = processFile(file, event.action.operations, event.action.pluginOptions)
    const tranforms = await Promise.all(results)

    const result = {
      type: 'JOB_COMPLETED',
      payload: {
        id: event.id,
        output: []
      }
    }

    await Promise.all(tranforms.map(async t => {
      const data = await fs.readFile(t.outputPath)
      result.payload.output.push({...t, data: data.toString('base64')})
    }))
    const messageId = await pubSubClient.topic(event.topic).publish(Buffer.from(JSON.stringify(result)))
    console.log("Published message ", messageId)
    await fs.emptyDir('/tmp')
  } catch (err) {
    const messageId = await pubSubClient.topic(event.topic).publish(Buffer.from(JSON.stringify({
      type: 'JOB_FAILED',
      payload: {
        id: event.id,
        error: err.toString()
      }
    })))
    console.err("Failed to process message:", messageId, err)
    await fs.emptyDir('/tmp')
  }
};