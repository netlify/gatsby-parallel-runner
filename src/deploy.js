#!/usr/bin/env node

const { spawn } = require('child_process');
const { readFile } = require('fs-extra')
const path = require('path')

exports.deploy = async function() {
  return new Promise(async (resolve, reject) => {
    const cwd = path.join(__dirname, '..', 'cloud_sharp_processor')
    const creds = await readFile(process.env.GOOGLE_APPLICATION_CREDENTIALS)
    const config = JSON.parse(creds)

    const ps = spawn('gcloud', [
      'functions', 'deploy', 'gatsbySharpProcessor', `--service-account=${config.client_email}`, '--project', config.project_id, '--runtime', 'nodejs10', '--verbosity', 'debug', '--trigger-topic', process.env.WORKER_TOPIC
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
