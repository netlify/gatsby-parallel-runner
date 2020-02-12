const fs = require('fs-extra')
const path = require('path')
const log = require('loglevel')

exports.process = async function(queue, msg) {
  if (!msg.inputPaths || msg.inputPaths.length > 1) {
    log.error("Wrong number of input paths in msg: ", msg)
    return Promise.reject('Wrong number of input paths')
  }

  const file = msg.inputPaths[0].path

  try {
    console.debug("Processing image", file)
    const output = await queue.process({id: msg.id, args: msg.args, file})
    console.debug("Got output from queue")
    await Promise.all(output.map(async (transform) => {
      const filePath = path.join(msg.outputDir, transform.outputPath)
      try {
        await fs.mkdirp(path.dirname(filePath))
      } catch(err) {
        return Promise.reject(`Failed making output directory: ${err}`)
      }
      console.debug("Writing tranform to file")
      return fs.writeFile(filePath, Buffer.from(transform.data, 'base64'))
    }))
  } catch (err) {
    log.error("Error during processing", err)
    return Promise.reject(err)
  }
}