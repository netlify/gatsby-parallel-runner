#!/usr/bin/env node

const util = require('util');
const exec = util.promisify(require('child_process').exec);

exports.deploy = async function() {
  await exec(`gcloud functions deploy gatsby-cloud-sharp-processor --runtime nodejs10 --trigger-topic ${process.env.WORKER_TOPIC}`)
}
