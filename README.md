# Gatsby Parallel Runner

This is an early WIP version of a runner for gatsby that can parallelize tasks like image processing.

When gatsby is started from a parent process with the environment variable `ENABLE_GATSBY_EXTERNAL_JOBS` set,
it will communicate some jobs up to the parent process via ipc, instead of running them in it's own internal
queue.

This allows a parent process to orchestrate certain task for better paralelization with cloud functions, etc.

This WIP will paralelize the sharp plugin image transformations to a Google Cloud Function via Google PubSub.

There's currently no versioning of the sharp plugin and no real error handling, but the basics are working.

## Intallation and usage

Install in your gatsby project:

```
npm i gatsby-parallel-runner
```


Set relevant env variables in your shell

```
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/your/google-credentials.json
export TOPIC=parallel-runner-topic
export WORKER_TOPIC=function-worker-topic
```

Deploy the cloud function

```
npm run deploy-sharp-worker


```


gatsby-parallel-runner
```

From within your gatsby project.