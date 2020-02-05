#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path')

exports.deploy = async function() {
  return new Promise((resolve, reject) => {
    const cwd = path.join(__dirname, '..', 'cloud_sharp_processor')
    console.log(cwd)
    const ps = spawn('gcloud', [
      'functions', 'deploy', 'gatsbySharpProcessor', '--runtime', 'nodejs10', '--verbosity', 'debug', '--trigger-topic', process.env.WORKER_TOPIC
    ], {
      shell: true,
      cwd,
      stdio: 'inherit'
    })

    ps.on('close', (code) => {
      process.exit(code)
    })
  })
}
