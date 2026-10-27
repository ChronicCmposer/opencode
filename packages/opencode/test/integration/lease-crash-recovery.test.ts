// P0: a hard-crashed worker generation never runs `dispose()`, so its worker
// lease stays live in the dead isolate until the 30 s TTL. The hub already knows
// the generation is retired (crash/terminate), so retirement must mark the lease
// dead; otherwise the immediate successor boot dies with `Bus.LeaseConflict`.
//
// A genuinely live generation must still be refused (covered by
// `owner-lease-production`); this test only proves the retired path.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { MessageID } from "@/session/schema"
import {
  createProductionSessionHub,
  releaseRetiredWorkerLease,
  type SessionHub,
} from "@/session/worker/hub"
import { createDbExecutor } from "@/session/worker/db-executor"
import { WorkerCrashedError } from "@/session/worker/dispatch"
import { sessionWorkerInit } from "../lib/session-worker-init"
import { workerSnapshot } from "../lib/snapshot"
import { TestLLMServer } from "../lib/llm-server"
import { testEffect } from "../lib/effect"
import { sid } from "../lib/async"
import { pollUntil } from "../lib/poll"
import { Effect } from "effect"

const it = testEffect(TestLLMServer.layer)
const SESSION_WORKER = new URL("../../src/session/worker/session-worker.ts", import.meta.url)
const directories: string[] = []

function freshWorkspace() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-lease-crash-recovery-"))
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
      ["project-lease-crash", "lease-crash", "/tmp/lease-crash", "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [sessionID, "project-lease-crash", "lease-crash", "/tmp/lease-crash", "lease-crash", "1", 1, 1],
    )
  } finally {
    connection.close()
  }
}

interface LeaseRow {
  readonly worker_owner_id: string | null
  readonly worker_expires_at: number | null
}

function readLease(filename: string, sessionID: string): LeaseRow | undefined {
  const connection = openConnection({ filename, busyTimeoutMs: 5000 })
  try {
    return connection.all<LeaseRow>(
      "SELECT worker_owner_id, worker_expires_at FROM event_sequence WHERE aggregate_id = ?",
      [sessionID],
    )[0]
  } finally {
    connection.close()
  }
}

describe("retired lease release is scoped to the retired generation", () => {
  test("a delayed retirement release never zeroes a successor's live lease", async () => {
    const { filename } = freshWorkspace()
    await Effect.runPromise(migrateLikeProduction(filename))
    const sessionID = "ses_scoped_release"
    seedSession(filename, sessionID)

    const connection = openConnection({ filename, busyTimeoutMs: 5_000 })
    try {
      connection.run(
        "INSERT INTO event_sequence (aggregate_id, seq, worker_owner_id, worker_expires_at) VALUES (?, ?, ?, ?)",
        [sessionID, 0, "worker-1-999-dead", Date.now() + 30_000],
      )
      // The successor reclaimed before the delayed retirement release ran.
      connection.run(
        "UPDATE event_sequence SET worker_owner_id = ?, worker_expires_at = ? WHERE aggregate_id = ?",
        ["worker-2-888-live", Date.now() + 30_000, sessionID],
      )
    } finally {
      connection.close()
    }

    const db = createDbExecutor({ filename, timeoutMs: 10_000 })
    try {
      const released = await releaseRetiredWorkerLease(db, sid(sessionID), "worker-1-999-dead")
      expect(released).toBe(false)
      const lease = readLease(filename, sessionID)
      expect(lease?.worker_owner_id).toBe("worker-2-888-live")
      expect(lease?.worker_expires_at).toBeGreaterThan(0)
    } finally {
      await db.close()
    }
  })
})

function dispatchPrompt(hub: SessionHub, sessionID: string, text: string): Promise<void> {
  return hub.dispatcher.dispatch({
    sessionID: sid(sessionID),
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: sid(sessionID),
      agent: "build",
      parts: [{ type: "text", text }],
    },
  })
}

async function crashPayload(run: Promise<void>, timeoutMs = 10_000) {
  const outcome = await Promise.race([
    run.then(
      () => ({ kind: "completed" as const }),
      (error: unknown) => ({ kind: "failed" as const, error }),
    ),
    new Promise<{ kind: "timeout" }>((resolve) => setTimeout(() => resolve({ kind: "timeout" }), timeoutMs)),
  ])
  if (outcome.kind === "completed") throw new Error("expected the run to crash, but it completed")
  if (outcome.kind === "timeout") throw new Error("timed out waiting for the worker crash")
  if (!(outcome.error instanceof WorkerCrashedError)) {
    throw new Error(`expected WorkerCrashedError, received ${String(outcome.error)}`)
  }
  return outcome.error.payload
}

interface RealHubFixture {
  readonly hub: SessionHub
  readonly dispose: () => Promise<void>
}

function startHub(input: {
  readonly llm: TestLLMServer["Service"]
  readonly filename: string
  readonly directory: string
  readonly env?: Record<string, string | undefined>
  readonly cancellation?: { readonly graceMs?: number }
}): RealHubFixture {
  const db = createDbExecutor({ filename: input.filename, timeoutMs: 10_000 })
  const hub = createProductionSessionHub({
    db,
    init: () =>
      ({
        ...sessionWorkerInit({
          snapshot: workerSnapshot({ directory: input.directory }),
          model: { baseUrl: input.llm.url, apiKey: "test-key" },
        }),
      }) as ReturnType<typeof sessionWorkerInit>,
    entry: SESSION_WORKER,
    databasePath: input.filename,
    env: { OPENCODE_DB: input.filename, ...(input.env ?? {}) },
    timeoutMs: 10_000,
    runIdleTimeoutMs: 3_000,
    ...(input.cancellation === undefined ? {} : { cancellation: input.cancellation }),
    ports: {
      hubProxy: {
        "sessionStatus.get": () => ({ type: "idle" }),
        "sessionStatus.list": () => new Map(),
        "sessionStatus.set": () => undefined,
        "permission.ask": () => undefined,
        "permission.reply": () => undefined,
        "permission.list": () => [],
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

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("P0: a crashed generation's lease is reclaimed immediately", () => {
  it.live(
    "the immediate successor claims after a hard crash without waiting for the TTL",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_lease_crash_recovery"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        // Turn one crashes at the tool result boundary; turn two is the
        // immediate successor and consumes the queued text.
        yield* llm.tool("todowrite", { todos: [{ content: "side effect", status: "pending", priority: "high" }] })
        yield* llm.text("after crash")

        const fixture = startHub({
          llm,
          filename,
          directory,
          env: { OPENCODE_TEST_FAULTS: "crashWorker@tool:result" },
        })
        try {
          const payload = yield* Effect.promise(() => crashPayload(dispatchPrompt(fixture.hub, sessionID, "write")))
          expect(payload?.phase).toBe("tool")

          // The dead isolate never ran `dispose()`; retirement marked the lease
          // reclaimable instead of leaving it live for 30 s. The release is a
          // hub-executor write, so wait for it to land.
          yield* Effect.promise(() =>
            pollUntil(() => readLease(filename, sessionID)?.worker_expires_at === 0, {
              message: "retirement never released the dead lease",
            }),
          )
          const deadLease = readLease(filename, sessionID)
          expect(deadLease?.worker_owner_id).toEqual(expect.any(String))
          expect(deadLease?.worker_expires_at).toBe(0)

          // The immediate successor boots and claims the same aggregate.
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, "after crash"))

          const successorLease = readLease(filename, sessionID)
          expect(successorLease?.worker_owner_id).toEqual(expect.any(String))
          expect(successorLease?.worker_owner_id).not.toBe(deadLease?.worker_owner_id)
          expect(successorLease?.worker_expires_at).toBeGreaterThan(0)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    30_000,
  )
})

describe("P0: retirement release is ordered before the successor boots", () => {
  it.live(
    "a cancelled generation's release lands before an immediate re-dispatch",
    () =>
      Effect.gen(function* () {
        const llm = yield* TestLLMServer
        const { filename, directory } = freshWorkspace()
        const sessionID = "ses_lease_cancel_redispatch"
        yield* migrateLikeProduction(filename)
        seedSession(filename, sessionID)
        // Turn one hangs so it can be cancelled while it owns the lease; turn
        // two is queued for the immediate successor.
        yield* llm.hang
        yield* llm.text("after cancel")

        const fixture = startHub({ llm, filename, directory, cancellation: { graceMs: 1 } })
        try {
          const firstRun = dispatchPrompt(fixture.hub, sessionID, "first").catch(() => undefined)
          yield* llm.wait(1)
          const retired = readLease(filename, sessionID)
          expect(retired?.worker_owner_id).toEqual(expect.any(String))

          const outcome = yield* Effect.promise(() => fixture.hub.cancel(sid(sessionID)))
          // A hung generation cannot unwind inside the 1 ms grace, so cancel
          // terminates and retires it (which is what releases the lease).
          expect(outcome?.mode).toBe("terminated")
          // Immediate re-dispatch: admission awaits the retired release, so the
          // successor claims after the old lease is dead.
          yield* Effect.promise(() => dispatchPrompt(fixture.hub, sessionID, "second"))
          void firstRun

          const successor = readLease(filename, sessionID)
          expect(successor?.worker_owner_id).toEqual(expect.any(String))
          expect(successor?.worker_owner_id).not.toBe(retired?.worker_owner_id)
          expect(successor?.worker_expires_at).toBeGreaterThan(0)
        } finally {
          yield* Effect.promise(() => fixture.dispose())
        }
      }),
    30_000,
  )
})
