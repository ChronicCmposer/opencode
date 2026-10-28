// Gate 7 — a flag-ON run must release its database transaction before it
// returns, so two back-to-back prompts in the SAME session both complete.
//
// A normal completion that left a transaction pinned past `message.completed`
// would make the immediate next prompt in that session fail. The pin release's
// unit contract lives in `test/session/worker/busy-retry.test.ts`; this is its
// prompt-level twin: the REAL `SessionPrompt.loop` over the per-worker WAL
// `Database`, run twice in one session.
//
// Each worker owns its connection (D5), so a leftover transaction shows up here
// as a failed `BEGIN IMMEDIATE` probe, not just a lock error. Every wait is
// bounded well below the old writer's 30s idle-rollback window.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Cause, Effect, Exit, Fiber } from "effect"
import { sql } from "drizzle-orm"
import { Database } from "@opencode-ai/core/database/database"
import { withBusyRetry } from "@opencode-ai/core/database/busy"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable, EventTable } from "@opencode-ai/core/event/sql"
import { openWriterConnection } from "@/session/writer/connection"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { InstanceRef } from "@/effect/instance-ref"
import { createSessionWork } from "@/session/worker/session-worker"
import { startMockProvider } from "../../lib/mock-provider"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { workerSnapshot } from "../../lib/snapshot"

/** A lock failure must fail the test with its own name, never look like a timeout. */
const LOCK_PATTERN = /database is locked|LockTimeoutError|SQLITE_BUSY|SQLITE_LOCKED/i

/** The writer's idle rollback window is 30s; a released pin settles in well under this. */
const PIN_RELEASE_BUDGET_MS = 5_000

/** Whole-prompt budget: generous for a mock provider, still far under 30s. */
const PROMPT_BUDGET_MS = 10_000

/**
 * Settle `promise` within `ms` or fail with `message`. A pinned transaction that
 * never releases surfaces as a bounded, descriptive failure instead of a hang.
 */
async function settlesWithin<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms)
  })
  try {
    return await Promise.race([promise, deadline])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

const mkModelConfig = (provider: { baseUrl: string; apiKey: string; model: string }) => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** Run an effect against the worker graph with the E2E instance context. */
const runInContext = <A, E, R>(ctx: E2EContext, effect: Effect.Effect<A, E, R>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance)))

/**
 * Probe the worker's database connection for a leftover transaction: `BEGIN`
 * fails loud when a transaction is still pinned, then `ROLLBACK` releases it.
 */
const probeWorkerConnection = async (ctx: E2EContext): Promise<void> => {
  const handle = await ctx.writer.begin()
  await ctx.writer.rollback(handle)
}

/** Create a session and its first user message through the worker's own services. */
const seedSession = (ctx: E2EContext, modelID: string, text: string): Promise<SessionID> =>
  runInContext(
    ctx,
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      const session = yield* sessions.create({
        title: "gate7",
        agent: "build",
        permission: [{ permission: "*", pattern: "*", action: "allow" }],
      })
      yield* appendUserMessage(sessions, session.id, modelID, text)
      return session.id
    }),
  )

/** Append one more user message to an existing session. */
const seedUserMessage = (ctx: E2EContext, sessionID: SessionID, modelID: string, text: string) =>
  runInContext(
    ctx,
    Effect.gen(function* () {
      const sessions = yield* Session.Service
      yield* appendUserMessage(sessions, sessionID, modelID, text)
    }),
  )

/** Persist the user message + its text part (the loop's input). */
function appendUserMessage(
  sessions: Session.Interface,
  sessionID: SessionID,
  modelID: string,
  text: string,
): Effect.Effect<void, never, Session.Service> {
  return Effect.gen(function* () {
    const user = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: user.id,
      sessionID,
      type: "text",
      text,
    } as never)
  })
}

/** The streamed assistant text from a completed `SessionPrompt.loop` result. */
function assistantText(result: unknown): string {
  const value = (result as { value: { parts: ReadonlyArray<{ type: string; text?: string }> } }).value
  return value.parts
    .filter((part) => part.type === "text")
    .map((part) => part.text ?? "")
    .join("")
}

/** Drizzle stores row payloads as JSON text; parse once at this boundary. */
const rowData = (row: Record<string, unknown>): Record<string, unknown> =>
  typeof row.data === "string" ? (JSON.parse(row.data) as Record<string, unknown>) : (row.data as Record<string, unknown>)

/**
 * Run one prompt, turning a lock failure into a named regression instead of an
 * opaque rejection, and bounding the whole run.
 */
async function runPrompt(
  work: ReturnType<typeof createSessionWork>,
  sessionID: SessionID,
  label: string,
): Promise<unknown> {
  const run = Effect.runPromise(work(sessionID)).catch((cause: unknown) => {
    const message = cause instanceof Error ? cause.message : String(cause)
    if (LOCK_PATTERN.test(message)) throw new Error(`${label} hit a writer lock after the previous run: ${message}`)
    throw cause
  })
  return settlesWithin(run, PROMPT_BUDGET_MS, `${label} did not settle within ${PROMPT_BUDGET_MS}ms`)
}

/** A fresh temp directory + provider + worker E2E context, torn down by the caller. */
async function withE2EContext(
  script: Parameters<typeof startMockProvider>[0],
  body: (ctx: E2EContext, provider: Awaited<ReturnType<typeof startMockProvider>>) => Promise<void>,
): Promise<void> {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-gate7-"))
  await using provider = startMockProvider(script)
  const ctx = createE2EContext({
    snapshot: workerSnapshot({ directory }),
    modelConfig: mkModelConfig(provider),
  })
  try {
    await body(ctx, provider)
  } finally {
    await ctx.dispose()
    fs.rmSync(directory, { recursive: true, force: true })
  }
}

describe("Gate 7 — sequential prompts in one session over the writer-backed worker", () => {
  test("two back-to-back prompts both complete and leave the writer unpinned", async () => {
    await withE2EContext({ scripts: [{ text: "first-reply" }, { text: "second-reply" }] }, async (ctx, provider) => {
      const sessionID = await seedSession(ctx, provider.model, "first prompt")
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })

      // 1. The first prompt completes with real assistant text.
      const first = await runPrompt(work, sessionID, "prompt 1")
      expect(assistantText(first)).toBe("first-reply")

      // 2. The worker connection is free the instant the run returns. An open
      //    transaction would make the probe's `BEGIN IMMEDIATE` fail.
      await settlesWithin(
        probeWorkerConnection(ctx),
        PIN_RELEASE_BUDGET_MS,
        "worker connection still pinned immediately after prompt 1 completed",
      )

      // 3. The next prompt starts immediately in the SAME session and completes.
      await seedUserMessage(ctx, sessionID, provider.model, "second prompt")
      const startedAt = performance.now()
      const second = await runPrompt(work, sessionID, "prompt 2")
      const secondDurationMs = performance.now() - startedAt

      expect(assistantText(second)).toBe("second-reply")
      // A pin only clears on the 30s idle timer; a healthy second prompt is
      // orders of magnitude faster.
      expect(secondDurationMs).toBeLessThan(PIN_RELEASE_BUDGET_MS)

      // 4. Both turns are durable exactly once: 2 user + 2 assistant messages,
      //    and each scripted reply appears in exactly one text part.
      const messages = ctx.rows("message").map(rowData)
      expect(messages.filter((message) => message.role === "user")).toHaveLength(2)
      expect(messages.filter((message) => message.role === "assistant")).toHaveLength(2)
      const textParts = ctx
        .rows("part")
        .map(rowData)
        .filter((part) => part.type === "text")
        .map((part) => part.text)
      expect(textParts.filter((text) => text === "first-reply")).toHaveLength(1)
      expect(textParts.filter((text) => text === "second-reply")).toHaveLength(1)
      expect(provider.requests).toHaveLength(2)
    })
  }, 120_000)

  // The deleted writer isolate owned whole-transaction busy retry and its pin
  // accounting (`SQLITE_BUSY_EXHAUSTED`, rollback/release on commit failure).
  // D5 re-homes that behavior in `@/database/transaction`; the contract is
  // covered by `test/session/worker/busy-retry.test.ts` and `test/session/bus.test.ts`.

  test("a read during the running prompt settles promptly and sees committed rows", async () => {
    await withE2EContext({ scripts: [{ text: "alpha" }, { text: "beta" }] }, async (ctx, provider) => {
      const sessionID = await seedSession(ctx, provider.model, "one")
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      await runPrompt(work, sessionID, "prompt 1")
      await seedUserMessage(ctx, sessionID, provider.model, "two")

      // Kick off prompt 2 and immediately read from the worker's own WAL
      // connection. WAL reads do not queue behind the run's write transaction and
      // must settle with the rows committed so far.
      const secondRun = runPrompt(work, sessionID, "prompt 2")
      const read = Effect.runPromise(ctx.database.all<{ n: number }>("SELECT COUNT(*) AS n FROM message"))
      const [second, rows] = await Promise.all([
        secondRun,
        settlesWithin(read, PIN_RELEASE_BUDGET_MS, "read starved behind the run's write transaction"),
      ])

      expect(assistantText(second)).toBe("beta")
      expect(Number(rows[0]?.n)).toBeGreaterThanOrEqual(2)
    })
  }, 120_000)
})

// ---------------------------------------------------------------------------
// Regression: a MAIN-process durable write must survive a session worker
// holding the SQLite write lock.
//
// D5 gives every worker its own WAL connection, so this test builds the two
// connections the failure needs over one real FILE:
//
//   * the session worker — its own `openWriterConnection` pins
//     `BEGIN IMMEDIATE` (the same lock semantics a running prompt holds), and
//   * the main process — `Database.layerFromPath` -> `Database.Service`,
//     issuing the durable write at the exact `event.ts` `commitDurableEvent`
//     seam: `db.transaction(..., { behavior: "immediate" })`.
//
// While the worker's `BEGIN IMMEDIATE` is pinned, the main transaction's own
// `BEGIN IMMEDIATE` hits `SQLITE_BUSY` -> `database is locked` /
// `LockTimeoutError`. The commit owner (`EventV2.commitDurableEvent`) wraps the
// whole transaction in `withBusyRetry`; this test mirrors that ownership so the
// bounded retry rides out the contention until the worker releases.
//
// `PRAGMA busy_timeout = 0` is applied to the main connection so the lock
// surfaces immediately (rather than after the production 5s wait) — the same
// technique the `effect-drizzle-sqlite` lock test uses. The contention itself
// is real: a second connection genuinely holds the write lock.

/** How long the worker holds its write lock while the main write contends. */
const MAIN_WRITE_CONTENTION_MS = 200

describe("Main-process durable write vs the session-worker write lock", () => {
  test("a main-process durable write settles instead of dying with database is locked", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-main-write-"))
    const dbPath = path.join(directory, "main.db")
    try {
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const { db } = yield* Database.Service
          // Fail fast on the lock instead of waiting the production `busy_timeout`.
          yield* db.run(sql`PRAGMA busy_timeout = 0`)

          // The session-worker connection: its own handle to the SAME file.
          const writerConnection = openWriterConnection(dbPath)
          yield* Effect.addFinalizer(() => Effect.sync(() => writerConnection.close()))

          // 1. Pin the write lock (as a running worker prompt would).
          writerConnection.exec("BEGIN IMMEDIATE")

          // 2. Concurrent main-process durable write.
          const write = db
            .transaction(
              () =>
                Effect.gen(function* () {
                  yield* db
                    .insert(EventSequenceTable)
                    .values([{ aggregate_id: "b-regression", seq: 0 }])
                    .run()
                  yield* db
                    .insert(EventTable)
                    .values([
                      {
                        id: EventV2.ID.create(),
                        aggregate_id: "b-regression",
                        seq: 0,
                        type: "regression",
                        data: {},
                      },
                    ])
                    .run()
                }),
              { behavior: "immediate" },
            )
            .pipe(withBusyRetry)
          const fiber = yield* write.pipe(Effect.forkScoped({ startImmediately: true }))

          // 3. Hold the lock while the main write contends, then release it.
          yield* Effect.sleep(MAIN_WRITE_CONTENTION_MS)
          writerConnection.exec("ROLLBACK")

          // 4. The main write must settle.
          const exit = yield* Fiber.join(fiber).pipe(Effect.exit)
          return Exit.isSuccess(exit)
            ? { settled: true as const, message: "" }
            : { settled: false as const, message: Cause.pretty(exit.cause) }
        }).pipe(Effect.provide(Database.layerFromPath(dbPath)), Effect.scoped),
      )

      expect(
        outcome.settled,
        outcome.settled ? "" : `main-process durable write died under writer contention:\n${outcome.message}`,
      ).toBe(true)
    } finally {
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 30_000)
})
