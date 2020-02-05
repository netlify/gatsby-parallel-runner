#!/usr/bin/env node

const { build } = require('../src/build')
const { deploy } = require('../src/deploy')
const { writeFileSync } = require('fs')

const requiredEnvVars = ['TOPIC', 'WORKER_TOPIC', 'GOOGLE_APPLICATION_CREDENTIALS']
requiredEnvVars.forEach(key => {
  if (!process.env[key]) {
    console.error(`You must set a ${key} environment variable`)
    process.exit(1)
  }
})

if (!process.env.GOOGLE_APPLICATION_CREDENTIALS.match(/\.json$/)) {
  const credentialsFile = '/tmp/credentials.json'
  let credentials = null
  try {
    credentials = atob(process.env.GOOGLE_APPLICATION_CREDENTIALS)
  } catch(err) {
    console.error("GOOGLE_APPLICATION_CREDENTIALS must either be a path to a .json file or base 64 encoded json credentials")
    process.exit(1)
  }
  writeFileSync(credentialsFile, credentials)
  process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsFile
}

const data = readFile

if (process.argv.length === 3 && process.argv[2] === 'deploy') {
  console.log("Deploying Cloud Worker")
  deploy()
} else {
  build()
}