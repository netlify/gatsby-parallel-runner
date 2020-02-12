#!/usr/bin/env node

const cp = require('child_process')
const log = require('loglevel')
const processorQueue = require('./processor-queue')
const imageProcessor = require('./image-processing')

exports.build = async function() {
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
    IMAGE_PROCESSING: 'IMAGE_PROCESSING'
  }

  process.env.ENABLE_GATSBY_EXTERNAL_JOBS = true

  const queue = await processorQueue.initialize()
  const gatsbyProcess = cp.fork(`${process.cwd()}/node_modules/.bin/gatsby`, ['build']);
  gatsbyProcess.on('exit', async (code) => {
    console.log("Gatsby is done")
    queue.stop()
    process.exit(code)
  });

  gatsbyProcess.on('message', async (msg) => {
    if (log.getLevel() <= log.levels.TRACE && msg.type !== MESSAGE_TYPES.LOG_ACTION) {
      log.trace("Got gatsby message", JSON.stringify(msg))
    }
    switch (msg.type) {
      case MESSAGE_TYPES.JOB_CREATED: {
        switch (msg.payload.name) {
          case JOB_TYPES.IMAGE_PROCESSING:
            try {
              await imageProcessor.process(queue, msg.payload)
            } catch (error) {
              log.error("Processing failed", msg.payload.id, error)
              gatsbyProcess.send({type: 'JOB_FAILED', payload: {id: msg.payload.id, error: error.toString()} })
            }
            return
          default:
            return gatsbyProcess.send({type: 'JOB_NOT_WHITELISTED', payload: {id: msg.payload.id}})
        }
      }
      case MESSAGE_TYPES.LOG_ACTION:
        // msg.action.payload.text && console.log(msg.action.payload.text)
        break
      default:
        log.warn("Ignoring message: ", msg)
    }
  });
}
