#!/usr/bin/env node

const { build } = require('../src/build')
const { deploy } = require('../src/deploy')

const requiredEnvVars = ['TOPIC', 'WORKER_TOPIC', 'GOOGLE_APPLICATION_CREDENTIALS']
requiredEnvVars.forEach(key => {
  if (!process.env[key]) {
    console.error(`You must set a ${key} environment variable`)
    process.exit(1)
  }
})

if (process.argv.length === 3 && process.argv[2] === 'deploy') {
  console.log("Deploying Cloud Worker")
  deploy()
} else {
  build()
}