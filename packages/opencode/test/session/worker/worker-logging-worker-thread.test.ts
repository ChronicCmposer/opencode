// A worker-thread emission still reaches the hub-side sink. The isolate runs the
// reduced session graph under `workerLoggingLayer`; its `Effect.log*` calls are
// rendered by the forwarding logger and written to the same `opencode.log` the
// hub uses. The per-session boundary annotates the log context, so the line the
// operator reads carries `session workers[<bare id>]:`.

import { describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Effect, Layer } from "effect"
import {
  ModelClient,
  PIPELINE_SESSION_NODES,
  buildSessionWorkerLayer,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { workerLoggingLayer } from "@/session/worker/worker-logging"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { workerSnapshot } from "../../lib/snapshot"

async function tempLog(): Promise<{ file: string; dispose: () => Promise<void> }> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "oc-worker-log-thread-"))
  return {
    file: path.join(dir, "opencode.log"),
    dispose: () => fs.rm(dir, { recursive: true, force: true }),
  }
}

async function readUntil(file: string, needle: string, timeoutMs = 5000): Promise<string> {
  const deadline = Date.now() + timeoutMs
  let text = ""
  while (Date.now() < deadline) {
    text = await fs.readFile(file, "utf8").catch(() => "")
    if (text.includes(needle)) return text
    await Bun.sleep(25)
  }
  return text
}

const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = () => Layer.succeed(ModelClient, { complete: () => Effect.succeed({ text: "worker text" }) })

describe("worker-thread emit reaches the hub log sink", () => {
  test("an annotated worker-graph log lands in the shared sink with the session prefix", async () => {
    const { file, dispose } = await tempLog()
    try {
      const built = buildSessionWorkerLayer({
        nodes: PIPELINE_SESSION_NODES,
        snapshot: workerSnapshot(),
        env: { values: {} },
        clients: stubClients(),
        modelClient: modelLayer(),
        logging: workerLoggingLayer({ file, id: "worker-thread" }),
      })

      await Effect.runPromise(
        Effect.gen(function* () {
          yield* Effect.logInfo(workerLogMessage("worker graph ready"), { workerID: 4 })
        }).pipe(
          Effect.annotateLogs({ sessionID: "ses_thread" }),
          Effect.provide(built.layer),
        ),
      )

      const text = await readUntil(file, "session workers[thread]: worker graph ready")
      expect(text).toContain("session workers[thread]: worker graph ready")
      // The full branded id is not truncated and the run scope is retained.
      expect(text).toContain("sessionID=ses_thread")
      expect(text).toContain("run=worker-thread")
    } finally {
      await dispose()
    }
  })
})
