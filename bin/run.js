#!/usr/bin/env node

const { build } = require('../src/build')
const { deploy } = require('../src/deploy')

if (!process.env.TOPIC) {
  console.error("You must set a TOPIC environment variable")
  process.exit(1)
}

if (!process.env.WORKER_TOPIC) {
  console.error("You must set a WORKER_TOPIC environment variable")
  process.exit(1)
}

if (process.argv.length === 3 && process.argv[2] === 'deploy') {
  console.log("Deploying Cloud Worker")
  deploy()
} else {
  build()
}