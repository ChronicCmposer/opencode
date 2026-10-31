// A long session's hub-proxied plugin hooks must not fail the turn on the worker frame cap.
//
// These drive the REAL production path (production hub, real session-worker isolate, real
// hub-proxy port) with a hub plugin that declares the hook under test. The payloads are made
// of many strings that are each below the per-string hold threshold, so only the whole-frame
// spill can carry them: the request (and for messages.transform the reply) is several MiB.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageID, PartID } from "@/session/schema"
import { createProductionSessionHub, type SessionHub } from "@/session/worker/hub"
import { createDbExecutor } from "@/session/worker/db-executor"
import { createTestBarrier } from "../lib/run-barrier-test"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import { TestLLMServer } from "../lib/llm-server"
import { testEffect } from "../lib/effect"
import { sid } from "../lib/async"
import {
  REAL_WORKER_IDLE_TIMEOUT_MS,
  REAL_WORKER_TEST_TIMEOUT_MS,
  REAL_WORKER_TRANSPORT_TIMEOUT_MS,
} from "../lib/worker-timeouts"
import { noMcpNoSnapshotHubProxy } from "../lib/hub-proxy-defaults"

const it = testEffect(TestLLMServer.layer)

const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const PART_CHARS = 30_000
const PART_COUNT = 200
const MIN_TOTAL_BYTES = 6_000_000
const directories: string[] = []

const mediumText = (index: number) => `${index}:`.padEnd(PART_CHARS, "x")

function freshWorkspace(): { readonly filename: string; readonly directory: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-hub-proxy-spill-"))
  directories.push(directory)
  return { filename: path.join(directory, "opencode.db"), directory }
}

function migrateLikeProduction(filename: string) {
  return Effect.scoped(
    Effect.gen(function* () {
      yield* Database.Service
    }),
  ).pipe(Effect.provide(Database.layerFromPath(filename)))
}

function seedSession(filename: string, sessionID: string): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      ["project-spill", "spill", "/tmp/spill", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-spill", "spill", "/tmp/spill", "spill", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

/** Seed `PART_COUNT` medium text parts across alternating user/assistant messages. */
function seedLongHistory(filename: string, sessionID: string, directory: string): void {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  const model = { providerID: "test-model", modelID: "test-model" }
  try {
    let time = 10
    let parentID = ""
    for (let index = 0; index < PART_COUNT; index++) {
      const messageID = MessageID.ascending()
      const user = index % 2 === 0
      if (user) parentID = messageID
      connection.run(
        "INSERT INTO message (id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?)",
        [
          messageID,
          sessionID,
          time,
          time,
          JSON.stringify(
            user
              ? { role: "user", time: { created: time }, agent: "build", model }
              : {
                  role: "assistant",
                  time: { created: time, completed: time },
                  parentID,
                  agent: "build",
                  mode: "build",
                  ...model,
                  path: { cwd: directory, root: directory },
                  cost: 0,
                  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
                  finish: "end_turn",
                },
          ),
        ],
      )
      connection.run("INSERT INTO part (id, message_id, session_id, time_created, time_updated, data) VALUES (?, ?, ?, ?, ?, ?)", [
        PartID.ascending(),
        messageID,
        sessionID,
        time,
        time,
        JSON.stringify({ type: "text", text: mediumText(index) }),
      ])
      time += 1
    }
  } finally {
    connection.close()
  }
}

function readMessages(filename: string, sessionID: string) {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return connection
      .all<{ readonly id: string; readonly data: string }>(
        "SELECT id, data FROM message WHERE session_id = ? ORDER BY time_created, id",
        [sessionID],
      )
      .map((row) => ({ id: row.id, ...(JSON.parse(row.data) as { role: string; error?: unknown }) }))
  } finally {
    connection.close()
  }
}

interface HookCall {
  readonly name: string
  readonly bytes: number
  readonly partCount: number
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
  readonly declared: readonly string[]
  readonly calls: HookCall[]
}) {
  const record = (name: string, payload: unknown, partCount: number) => {
    input.calls.push({ name, bytes: JSON.stringify(payload).length, partCount })
  }
  const db = createDbExecutor({ filename: input.filename, timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS })
  const hub = createProductionSessionHub({
    barrier: createTestBarrier(),
    db,
    init: () =>
      ({
        ...sessionWorkerInit({
          snapshot: workerSnapshot({ directory: input.directory }),
          model: { baseUrl: input.llm.url, apiKey: "test-key" },
        }),
        plugins: [{ name: "spill-e2e", hooks: {}, proxied: input.declared }],
      }) as ReturnType<typeof sessionWorkerInit>,
    entry: SESSION_WORKER,
    databasePath: input.filename,
    env: { OPENCODE_DB: input.filename },
    timeoutMs: REAL_WORKER_TRANSPORT_TIMEOUT_MS,
    runIdleTimeoutMs: REAL_WORKER_IDLE_TIMEOUT_MS,
    ports: {
      hubProxy: {
        ...noMcpNoSnapshotHubProxy,
        "sessionStatus.get": () => ({ type: "idle" }),
        "sessionStatus.list": () => new Map(),
        "sessionStatus.set": () => undefined,
        "permission.ask": () => undefined,
        "permission.reply": () => undefined,
        "permission.list": () => [],
        messagesTransform: async (_input, output) => {
          const messages = (output as { messages: Array<{ parts: unknown[] }> }).messages
          record(
            "experimental.chat.messages.transform",
            output,
            messages.reduce((total, message) => total + message.parts.length, 0),
          )
          return output
        },
        pluginTrigger: async (name, pluginInput, output) => {
          const parts = (output as { parts?: unknown[] }).parts
          record(name as string, [pluginInput, output], parts?.length ?? 0)
          return output
        },
      },
    },
  })
  return {
    hub,
    dispose: async () => {
      await hub.terminateAll()
      await db.close()
    },
  }
}

function dispatchPrompt(hub: SessionHub, sessionID: string, parts: ReadonlyArray<{ type: "text"; text: string }>) {
  return hub.dispatcher.dispatch({
    sessionID: sid(sessionID),
    prompt: { messageID: MessageID.ascending(), sessionID: sid(sessionID), agent: "build", parts: [...parts] },
  })
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("hub-proxied plugin hooks with multi-MiB payloads of medium strings", () => {
  it.live(
    "experimental.chat.messages.transform receives a 6 MB history made of 30K parts and the turn commits",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_spill_history"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        seedLongHistory(filename, sessionID, directory)
        yield* llm.text("done")

        const calls: HookCall[] = []
        const fixture = startHub({
          llm,
          filename,
          directory,
          declared: ["experimental.chat.messages.transform"],
          calls,
        })
        try {
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, [{ type: "text", text: "next" }]))
          const seen = calls.find((call) => call.name === "experimental.chat.messages.transform")
          expect(seen).toBeDefined()
          expect(seen!.bytes).toBeGreaterThanOrEqual(MIN_TOTAL_BYTES)
          expect(seen!.partCount).toBeGreaterThanOrEqual(PART_COUNT)
          const messages = readMessages(filename, sessionID)
          const assistant = messages.filter((message) => message.role === "assistant").at(-1)!
          expect(messages.indexOf(assistant)).toBe(messages.length - 1)
          expect(assistant.error).toBeUndefined()
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "chat.message receives a 6 MB prompt made of 30K text parts and the turn commits",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_spill_chat_message"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("done")

        const calls: HookCall[] = []
        const fixture = startHub({ llm, filename, directory, declared: ["chat.message"], calls })
        try {
          const parts = Array.from({ length: PART_COUNT }, (_, index) => ({
            type: "text" as const,
            text: mediumText(index),
          }))
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, parts))
          const seen = calls.find((call) => call.name === "chat.message")
          expect(seen).toBeDefined()
          expect(seen!.bytes).toBeGreaterThanOrEqual(MIN_TOTAL_BYTES)
          expect(seen!.partCount).toBeGreaterThanOrEqual(PART_COUNT)
          const assistant = readMessages(filename, sessionID).filter((message) => message.role === "assistant").at(-1)
          expect(assistant).toBeDefined()
          expect(assistant!.error).toBeUndefined()
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )

  it.live(
    "chat.message receives one 6 MB text part and the turn commits",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_spill_single_part"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        yield* llm.text("done")

        const calls: HookCall[] = []
        const fixture = startHub({ llm, filename, directory, declared: ["chat.message"], calls })
        try {
          const text = "y".repeat(MIN_TOTAL_BYTES)
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, [{ type: "text", text }]))
          const seen = calls.find((call) => call.name === "chat.message")
          expect(seen).toBeDefined()
          expect(seen!.bytes).toBeGreaterThanOrEqual(MIN_TOTAL_BYTES)
          const assistant = readMessages(filename, sessionID).filter((message) => message.role === "assistant").at(-1)
          expect(assistant).toBeDefined()
          expect(assistant!.error).toBeUndefined()
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    REAL_WORKER_TEST_TIMEOUT_MS,
  )
})
