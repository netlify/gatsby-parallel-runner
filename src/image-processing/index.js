const fs = require('fs-extra')
const path = require('path')
const log = require('loglevel')

exports.process = async function(processor, msg) {
  if (!msg.inputPaths || msg.inputPaths.length > 1) {
    log.error("Wrong number of input paths in msg: ", msg)
    return Promise.reject('Wrong number of input paths')
  }

  const file = msg.inputPaths[0].path

  try {
    log.debug("Processing image", file)
    const result = await processor.process({id: msg.id, args: msg.args, file})
    log.debug("Got output from processing")
    return await Promise.all(result.output.map(async (transform) => {
      const filePath = path.join(msg.outputDir, transform.outputPath)
      try {
        await fs.mkdirp(path.dirname(filePath))
      } catch(err) {
        return Promise.reject(`Failed making output directory: ${err}`)
      }
      log.debug("Writing tranform to file")
      await fs.writeFile(filePath, Buffer.from(transform.data, 'base64'))
      return {outputPath: transform.outputPath, args: transform.args}
    }))
  } catch (err) {
    log.error("Error during processing", err)
    return Promise.reject(err)
  }
}