import { describe, expect, it, spyOn } from "bun:test"
import { existsSync } from "node:fs"
import type { WorkerModelConfig } from "@/session/worker/layer"
import { withTempDir, withWorkerE2E } from "./e2e-context"

const modelConfig: WorkerModelConfig = {
  providerID: "mock",
  modelID: "mock",
  apiId: "mock",
  npm: "@ai-sdk/openai-compatible",
  baseUrl: "http://127.0.0.1:1",
  apiKey: "test-key",
  headers: {},
}

describe("withTempDir", () => {
  it("cleans the directory after the callback resolves", async () => {
    let seen = ""
    await withTempDir(async (tmp) => {
      seen = tmp.path
      expect(existsSync(seen)).toBe(true)
    })
    expect(existsSync(seen)).toBe(false)
  })

  it("cleans the directory when the callback throws", async () => {
    let seen = ""
    await expect(
      withTempDir(async (tmp) => {
        seen = tmp.path
        throw new Error("callback boom")
      }),
    ).rejects.toThrow("callback boom")
    expect(existsSync(seen)).toBe(false)
  })
})

describe("withWorkerE2E", () => {
  it("owns the temp dir and E2E context, cleaning both after success", async () => {
    let directory = ""
    let writerDispose: ReturnType<typeof spyOn> | undefined
    const result = await withWorkerE2E({ modelConfig }, async (e2e) => {
      directory = e2e.directory
      expect(existsSync(directory)).toBe(true)
      writerDispose = spyOn(e2e.context.writer, "dispose")
      return "ok"
    })

    expect(result).toBe("ok")
    expect(writerDispose).toHaveBeenCalledTimes(1)
    expect(existsSync(directory)).toBe(false)
  })

  it("cleans the temp dir when context construction fails", async () => {
    let directory = ""
    await expect(
      withWorkerE2E(
        {
          modelConfig,
          createContext: (options) => {
            directory = options.snapshot.directory
            throw new Error("construction boom")
          },
        },
        async () => {},
      ),
    ).rejects.toThrow("construction boom")

    expect(directory).not.toBe("")
    expect(existsSync(directory)).toBe(false)
  })
})
