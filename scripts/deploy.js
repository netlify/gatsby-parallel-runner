#!/usr/bin/env node

const { exec } = require("child_process");

if (!process.env.WORKER_TOPIC) {
  console.error("You must set a WORKER_TOPIC environment variable")
  process.exit(1)
}

exec(`gcloud functions deploy gatsby-cloud-sharp-processor --runtime nodejs10 --trigger-topic ${process.env.WORKER_TOPIC}`)