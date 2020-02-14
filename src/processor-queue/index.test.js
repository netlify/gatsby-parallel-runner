'use strict';

const path = require('path')
const { Processor, Job, GooglePubSub } = require('./index');

process.env.TOPIC = 'test'

test('Job size calculation for string', async () => {
  const job = await new Job({id: "1234", args: [], file: Buffer.from("Hello, World")})
  expect(job.fileSize).toBe(12)
})

test('Job size calculation for file', async () => {
  const job = await new Job({id: "1234", args: [], file: path.join(__dirname, 'test', 'hello.txt')})
  expect(job.fileSize).toBe(14)
})

test('Job size calculation for missing file', async () => {
  const file = path.join(__dirname, 'test', 'nopes.txt')
  expect.assertions(1)
  await expect(new Job({id: "1234", args: [], file})).rejects.toThrow()
})

test('job message for string', async () => {
  const job = await new Job({id: "1234", args: [], file: Buffer.from("Hello, World")})
  const msg = await job.msg()
  expect(msg).toBeInstanceOf(Buffer)
  expect(JSON.parse(msg.toString())).toEqual({
    id: "1234",
    action: [],
    file: Buffer.from("Hello, World").toString("base64"),
    topic: 'test'
  })
})

test('job message for file', async () => {
  const file = path.join(__dirname, 'test', 'hello.txt')
  const job = await new Job({id: "1234", args: [], file})
  const msg = await job.msg()
  expect(msg).toBeInstanceOf(Buffer)
  expect(JSON.parse(msg.toString())).toEqual({
    id: "1234",
    action: [],
    file: Buffer.from("Hello, World!\n").toString("base64"),
    topic: 'test'
  })
})

// test('process should push a job unto the queue', async () => {
//   expect.assertions(2)
//   const pubSubImplementation = {
//     publish: (msg) => { expect(msg).toBeDefined()},
//     subscribe: (handler) => { setTimeout(() => handler({
//       id: "2345", type: "JOB_COMPLETED", payload: {id: "1234", output: "done"}
//     }))}
//   }
//   console.log("Instantiating processor for success test")
//   const processor = new Processor({pubSubImplementation})
//   console.log("Processing for success test")
//   const result = await processor.process({id: "1234", args: [], file: Buffer.from("Hello")})
//   expect(result).toEqual({id: "1234", output: "done"})
// })


test('failure message should cancel processing', async () => {
  expect.assertions(2)
  const pubSubImplementation = {
    publish: (msg) => { expect(msg).toBeDefined()},
    subscribe: (handler) => { setTimeout(() => handler({
      id: "2345", type: "JOB_FAILED", payload: {id: "1234", error: "Error"}
    }))}
  }
  const processor = new Processor({pubSubImplementation})
  try {
    await processor.process({id: "1234", args: [], file: Buffer.from("Hello")})
  } catch(err) {
    expect(err).toBeDefined()
  }
})

test('instantiate google pubsub', async () => {
  const pubSub = await new GooglePubSub({noSubscription: true})
  expect(pubSub).toBeInstanceOf(GooglePubSub)
})

test('size check for google publish', async () => {
  const pubSub = await new GooglePubSub({noSubscription: true})
  const msg = Buffer.from('Hello, World!')
  pubSub.maxPubSubSize = 10000
  pubSub.pubSubClient = {topic: () => { return {
    publish: async (msg) => { expect(msg).toBe(msg) }
  } }}
  pubSub.storageClient = {bucket: () => { return {
    file: (path) => {
      expect(path).toEqual(`event-2345`)
      return {save: async (data, options) => {
        expect(Buffer.from(data, 'base64').toString()).toEqual('Hello, World!')
        expect(options).toEqual({resumable: false})
      }
    }}
  } }}
  pubSub.publish("1234", msg)

  pubSub.maxPubSubSize = 2
  pubSub.publish("2345", msg)
})

