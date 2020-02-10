#!/usr/bin/env node

const { spawn } = require('child_process');
const { readFile } = require('fs-extra')
const path = require('path')
const { PubSub } = require('@google-cloud/pubsub')
const { Storage } = require('@google-cloud/storage')

const bucketName = `event-processing-${process.env.WORKER_TOPIC}`
const resultBucketName = `event-results-${process.env.TOPIC}`

function deployType(type, cwd, config) {
  return new Promise((resolve, reject) => {
    const args =[
      'functions', 'deploy', `gatsbySharpProcessor${type}`, '--entry-point', 'gatsbySharpProcessor', '--memory', '1024MB',
      '--service-account', config.client_email, '--project', config.project_id,
      '--runtime', 'nodejs10', '--verbosity', 'debug'
    ]
    if (type === 'PubSub') {
      args.push('--trigger-topic')
      args.push(process.env.WORKER_TOPIC)
    } else {
      args.push('--trigger-resource')
      args.push(bucketName)
      args.push('--trigger-event google.storage.object.finalize')
    }

    const ps = spawn('gcloud', args, {shell: true, cwd, stdio: 'inherit'})

    ps.on('close', (code) => {
      if (code === 0) { return resolve(code) }
      reject(code)
    })
  })
}

exports.deploy = async function() {
  const cwd = path.join(__dirname, '..', 'cloud_sharp_processor')
  const creds = await readFile(process.env.GOOGLE_APPLICATION_CREDENTIALS)
  const config = JSON.parse(creds)

  const pubSubClient = new PubSub({
    projectId: config.project_id
  });
  const storage = new Storage({
    projectId: config.project_id
  });

  try {
    await pubSubClient.createTopic(process.env.WORKER_TOPIC)
  } catch(err) {
    console.log("Create topic failed", err)
  }

  try {
    const lifeCycle = `<?xml version="1.0" ?>
    <LifecycleConfiguration>
        <Rule>
            <Action>
                <Delete/>
            </Action>
            <Condition>
                <Age>30</Age>
            </Condition>
        </Rule>
    </LifecycleConfiguration>`
    const [bucket] = await storage.createBucket(bucketName);
    await bucket.setMetadata({lifeCycle});
  } catch(err) {
    console.log("Create bucket failed", err)
  }

  try {
    const lifeCycle = `<?xml version="1.0" ?>
    <LifecycleConfiguration>
        <Rule>
            <Action>
                <Delete/>
            </Action>
            <Condition>
                <Age>30</Age>
            </Condition>
        </Rule>
    </LifecycleConfiguration>`
    const [bucket] = await storage.createBucket(resultBucketName);
    await bucket.setMetadata({lifeCycle});
  } catch(err) {
    console.log("Create result bucket failed", err)
  }

  try {
    console.log("Deploying as pubsub handler")
    await deployType('PubSub', cwd, config)

    console.log("Deploying as storage handler")
    await deployType('Storage', cwd, config)
  } catch (code) {
    console.log("Error: ", code)
    process.exit(code)
  }
}
