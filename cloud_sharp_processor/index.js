const fs = require('fs')
const { processFile } = require('gatsby-plugin-sharp/process-file')
const {PubSub} = require('@google-cloud/pubsub');

const pubSubClient = new PubSub();

process.chdir('/tmp')

exports.gatsbySharpProcessor = async (msg, context) => {
  const pubSubMessage = JSON.parse(Buffer.from(msg.data, 'base64').toString());
  const file = Buffer.from(pubSubMessage.file)

  const results = processFile(file, pubSubMessage.action.operations, pubSubMessage.action.pluginOptions)
  const tranforms = await Promise.all(results)

  const result = {
    id: pubSubMessage.id,
    output: []
  }

  try {
    await Promise.all(tranforms.map(t => new Promise((resolve, reject) => {
      fs.readFile(t.outputPath, (err, data) => {
        if (err) { reject(err); }
        result.output.push({...t, data: data.toString()})
        resolve(true)
      })
    })))
    const messageId = await pubSubClient.topic(pubSubMessage.topic).publish(Buffer.from(JSON.stringify(result)))
    console.log("Published message ", messageId)
  } catch (err) {
    console.err("Fail!", err)
  }
};