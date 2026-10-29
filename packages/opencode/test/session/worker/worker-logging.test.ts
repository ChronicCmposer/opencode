// TDD — worker log forwarding. The session-worker isolate runs Effect code (the
// reduced session graph, the `SessionPrompt` loop, and the run-failure path) but
// owns no logger, so its `Effect.log*` lines vanish into the default no-op. This
// suite pins the four surfaces that make them reach the main `opencode.log`:
//
//   (a) `workerLoggingLayer` attaches the main file sink (unit);
//   (b) a worker graph built with `logging` captures logs emitted inside it;
//   (c) `workerLogBridge(snapshot, logging)` routes an `Effect.log*` event to
//       the sink with the `session workers[...]` prefix;
//   (d) a `createSessionWork` run that dies logs one Error line with the
//       sessionID + cause before the original defect is re-raised.

import { describe, expect, test } from "bun:test"
import fs from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { Effect, Layer } from "effect"
import { workerLoggingLayer } from "@/session/worker/worker-logging"
import {
  ModelClient,
  PIPELINE_SESSION_NODES,
  buildSessionWorkerLayer,
  type SessionLayerOptions,
} from "@/session/worker/layer"
import { SessionPrompt } from "@/session/prompt"
import { SessionID } from "@/session/schema"
import { createSessionWork, workerLogBridge } from "@/session/worker/session-worker"
import { workerLogMessage } from "@/session/worker/worker-logging"
import { workerSnapshot } from "../../lib/snapshot"

const sid = (value: string) => SessionID.make(value)

/** A fresh temp dir with an `opencode.log` path inside it. */
async function tempLog(): Promise<{ file: string; dispose: () => Promise<void> }> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "oc-worker-log-"))
  return {
    file: path.join(dir, "opencode.log"),
    dispose: () => fs.rm(dir, { recursive: true, force: true }),
  }
}

/**
 * Read the sink until it contains `needle` (the file sink flushes asynchronously
 * through a batch window), then return the accumulated text either way so a
 * failed expectation can show what actually landed.
 */
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

describe("worker logging (a): workerLoggingLayer file sink", () => {
  test("captures Effect.logInfo and Effect.logError into the sink file", async () => {
    const { file, dispose } = await tempLog()
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          yield* Effect.logInfo("worker info line", { slot: 1 })
          yield* Effect.logError("worker error line", { reason: "boom" })
        }).pipe(Effect.provide(workerLoggingLayer({ file, id: "worker-a" }))),
      )

      const text = await readUntil(file, "worker error line")
      expect(text).toContain("worker info line")
      expect(text).toContain("level=INFO")
      expect(text).toContain("worker error line")
      expect(text).toContain("level=ERROR")
      expect(text).toContain("run=worker-a")
    } finally {
      await dispose()
    }
  })
})

describe("worker logging (b): worker graph logging layer", () => {
  test("captures Effect.log* emitted inside the built worker layer", async () => {
    const { file, dispose } = await tempLog()
    try {
      const built = buildSessionWorkerLayer({
        nodes: PIPELINE_SESSION_NODES,
        snapshot: workerSnapshot(),
        env: { values: {} },
        clients: stubClients(),
        modelClient: modelLayer(),
        logging: workerLoggingLayer({ file, id: "worker-graph" }),
      })

      await Effect.runPromise(
        Effect.gen(function* () {
          yield* Effect.logInfo("inside worker graph", { slot: 2 })
        }).pipe(Effect.provide(built.layer)),
      )

      const text = await readUntil(file, "inside worker graph")
      expect(text).toContain("inside worker graph")
      expect(text).toContain("run=worker-graph")
    } finally {
      await dispose()
    }
  })
})

describe("worker logging (c): worker log bridge forwarding", () => {
  test("writes the session workers[...] line to the sink file", async () => {
    const { file, dispose } = await tempLog()
    try {
      const bridge = workerLogBridge(workerSnapshot(), workerLoggingLayer({ file, id: "worker-bridge" }))
      bridge.fork(Effect.logInfo(workerLogMessage("pool resolved"), { sessionID: "ses_abc" }))

      const text = await readUntil(file, "session workers[abc]: pool resolved")
      expect(text).toContain("session workers[abc]: pool resolved")
      expect(text).toContain("run=worker-bridge")
    } finally {
      await dispose()
    }
  })
})

describe("worker logging (d): createSessionWork failure logging", () => {
  const failingPromptLayer = (file: string): Layer.Layer<unknown, unknown> =>
    Layer.merge(
      Layer.succeed(SessionPrompt.Service, {
        cancel: () => Effect.succeed(undefined),
        prompt: () => Effect.die("prompt exploded"),
        loop: () => Effect.die("loop exploded"),
        shell: () => Effect.die("shell is not exercised"),
        command: () => Effect.die("command is not exercised"),
        resolvePromptParts: () => Effect.succeed([]),
      }) as unknown as Layer.Layer<unknown, unknown>,
      workerLoggingLayer({ file, id: "worker-fail" }),
    )

  test("a run that dies logs one Error line with the sessionID and cause, then re-raises", async () => {
    const { file, dispose } = await tempLog()
    try {
      const work = createSessionWork({
        layer: failingPromptLayer(file),
        writer: stubClients().writer,
        threadId: 0,
        prompt: true,
      })

      const error = await Effect.runPromise(work(sid("ses_fail"))).then(
        () => undefined,
        (cause: unknown) => cause,
      )
      // The original defect is preserved (not swallowed by the logger).
      expect(error).toBeDefined()

      const text = await readUntil(file, "session worker run failed")
      const lines = text.split("\n").filter((line) => line.includes("session worker run failed"))
      expect(lines).toHaveLength(1)
      expect(lines[0]).toContain("level=ERROR")
      expect(lines[0]).toContain("ses_fail")
      expect(lines[0]).toContain("loop exploded")
      expect(text).toContain("run=worker-fail")
    } finally {
      await dispose()
    }
  })
})
