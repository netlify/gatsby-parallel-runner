# Gatsby Parallel Runner

This is an early a parallel runner for gatsby that allows plugins and core parts of Gatsby to parallelize
suited tasks such as image processing.

When gatsby is started from a parent process with the environment variable `ENABLE_GATSBY_EXTERNAL_JOBS` set,
it will communicate some jobs up to the parent process via ipc, instead of running them in it's own internal
queue.

This allows a parent process to orchestrate certain task across mutiple workers for better paralelization
through autoscaling cloud functions or the like.

Currently this plugin includes a processing queue implementation based on Google Cloud Functions, but the
general abstractions in place should make it easy to add similar runtimes for other cloud providers or via
different approaches to parallelization.

## Intallation and usage

Install in your gatsby project:

```
npm i gatsby-parallel-runner
```

To use with Google Cloud, set relevant env variables in your shell:

```
export GOOGLE_APPLICATION_CREDENTIALS=~/path/to/your/google-credentials.json
export TOPIC=parallel-runner-topic
export WORKER_TOPIC=function-worker-topic
```

Deploy the cloud function:

```
npx gatsby-parallel-runner deploy

```

Then run your Gatsby build with the parallel runner instead of the default `gatsby build` command.

```
npx gatsby-parallel-runner
```
