const {
  deploy,
} = require("./processor-queue/implementations/google-pub-sub/deploy")

exports.deploy = async function() {
  try {
    await deploy()
  } catch (err) {
    console.error("Failed to deploy parallel functions", err)
    process.exit(1)
  }
}
