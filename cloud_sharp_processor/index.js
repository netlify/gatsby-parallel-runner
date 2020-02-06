const fs = require('fs')
const { processFile } = require('gatsby-plugin-sharp/process-file')
const {PubSub} = require('@google-cloud/pubsub');

const pubSubClient = new PubSub();

process.chdir('/tmp')

exports.gatsbySharpProcessor = async (msg, context) => {
  const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
  try {
    const file = Buffer.from(pubSubMessage.file)

    const results = processFile(file, pubSubMessage.action.operations, pubSubMessage.action.pluginOptions)
    const tranforms = await Promise.all(results)

    const result = {
      type: 'JOB_COMPLETED',
      payload: {
        id: pubSubMessage.id,
        output: []
      }
    }

    await Promise.all(tranforms.map(t => new Promise((resolve, reject) => {
      fs.readFile(t.outputPath, (err, data) => {
        if (err) { reject(err); }
        result.payload.output.push({...t, data: data.toString('base64')})
        resolve(true)
      })
    })))
    const messageId = await pubSubClient.topic(pubSubMessage.topic).publish(Buffer.from(JSON.stringify(result)))
    console.log("Published message ", messageId)
  } catch (err) {
    const messageId = await pubSubClient.topic(pubSubMessage.topic).publish(Buffer.from(JSON.stringify({
      type: 'JOB_FAILED',
      payload: {
        id: pubSubMessage.id,
        error: err.toString()
      }
    })))
    console.err("Failed to process message:", messageId, err)
  }
};