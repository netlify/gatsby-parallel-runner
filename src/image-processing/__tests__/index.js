const path = require("path")
const fs = require("fs-extra")
const { process } = require("../index")

test("test image processor", async () => {
  await fs.remove("/tmp/gatsby-parallel-transformed-image.png")
  const mockProcessor = {
    process: jest.fn(async msg => {
      expect(msg).toEqual({
        id: "1234",
        args: [],
        file: path.join(__dirname, "images", "gatsby-astronaut.png"),
      })
      return {
        files: {
          "gatsby-parallel-transformed-image.png": Buffer.from(
            "bogus data"
          ).toString("base64"),
        },
        output: [
          {
            outputPath: "gatsby-parallel-transformed-image.png",
            args: [],
          },
        ],
      }
    }),
  }
  await process(mockProcessor, {
    id: "1234",
    name: "IMAGE_PROCESSING",
    args: [],
    outputDir: "/tmp",
    inputPaths: [
      { path: path.join(__dirname, "images", "gatsby-astronaut.png") },
    ],
  })

  const data = await fs.readFile("/tmp/gatsby-parallel-transformed-image.png")
  expect(data.toString()).toEqual("bogus data")
})
