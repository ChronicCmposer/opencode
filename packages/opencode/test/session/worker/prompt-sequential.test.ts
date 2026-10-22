// Gate 7 — a flag-ON run must release its writer transaction before it returns,
// so two back-to-back prompts in the SAME session both complete.
//
// Before the `session/writer/core.ts` `finishTransaction` fix, a flag-ON normal
// completion could leave a transaction pinned past `message.completed`. The
// immediate next prompt in that session then queued behind the pin (and in the
// packaged binary hit `SQLiteError: database is locked` / `LockTimeoutError`).
// The pin release is unit-tested in `session/writer/core.test.ts`; this is its
// prompt-level twin: the REAL `SessionPrompt.loop` over the writer-backed worker
// `Database`, run twice in one session.
//
// The writer serializes everything through one queue and only runs an ordinary
// statement (or a `begin`) while no transaction is pinned, so a leftover pin
// shows up here as a bounded hang, not just a lock error. Every wait is bounded
// well below the writer's 30s idle-rollback window: a prompt that only succeeds
// because that timer fired is still a regression.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect } from "effect"
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

      // 2. The writer is free the instant the run returns. `begin` only runs when
      //    no transaction is pinned; a leftover pin would queue it for the full
      //    idle-rollback window.
      const probe = await settlesWithin(
        ctx.writer.begin(),
        PIN_RELEASE_BUDGET_MS,
        "writer still pinned immediately after prompt 1 completed",
      )
      await ctx.writer.rollback(probe)

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

  test("a failed COMMIT still releases the writer pin across the writer port", async () => {
    await withE2EContext({ scripts: [{ text: "unused" }] }, async (ctx) => {
      // Construct the production lock condition locally: a pinned transaction
      // whose COMMIT fails with `database is locked`. The fix's `finishTransaction`
      // rolls back and releases the pin in a `finally`; before it, the pin stayed
      // held and every queued operation starved until the 30s idle rollback.
      const originalRun = ctx.connection.run.bind(ctx.connection)
      let failCommit = true
      ctx.connection.run = ((sql: string, params?: ReadonlyArray<unknown>) => {
        if (failCommit && /^\s*commit\b/i.test(sql)) throw new Error("SQLiteError: database is locked")
        return originalRun(sql, params)
      }) as typeof ctx.connection.run

      const handle = await settlesWithin(ctx.writer.begin(), PIN_RELEASE_BUDGET_MS, "first begin did not settle")
      await settlesWithin(
        ctx.writer.txQuery(handle, "run", "INSERT INTO event (id, aggregate_id, seq, type, data) VALUES (?, ?, ?, ?, ?)", [
          "evt_gate7",
          "a",
          1,
          "t",
          "{}",
        ]),
        PIN_RELEASE_BUDGET_MS,
        "txQuery did not settle",
      )
      const commitError = await settlesWithin(
        ctx.writer.commit(handle).then(
          () => undefined,
          (cause: unknown) => cause,
        ),
        PIN_RELEASE_BUDGET_MS,
        "commit did not settle",
      )
      expect(commitError).toBeInstanceOf(Error)
      expect((commitError as Error).message).toMatch(/database is locked/i)
      failCommit = false

      // The failed commit must not leave the writer pinned: a queued read settles
      // promptly, and a fresh transaction can open on the recovered connection.
      const read = await settlesWithin(
        ctx.writer.query("all", "SELECT COUNT(*) AS n FROM event", []),
        PIN_RELEASE_BUDGET_MS,
        "writer stayed pinned after a failed COMMIT",
      )
      expect(read).toBeDefined()
      const next = await settlesWithin(
        ctx.writer.begin(),
        PIN_RELEASE_BUDGET_MS,
        "begin starved behind a failed COMMIT",
      )
      await ctx.writer.rollback(next)
    })
  }, 120_000)

  test("a read queued behind the running prompt settles promptly once the run ends", async () => {
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

      // Kick off prompt 2 and immediately queue a read behind whatever writer
      // transaction the run takes. The writer serializes both; the read must
      // settle once the run finishes, not after the idle rollback timer.
      const secondRun = runPrompt(work, sessionID, "prompt 2")
      const read = ctx.writer.query("all", "SELECT COUNT(*) AS n FROM message", []) as Promise<
        ReadonlyArray<{ n: number }>
      >
      const [second, rows] = await Promise.all([
        secondRun,
        settlesWithin(read, PIN_RELEASE_BUDGET_MS, "read starved behind the run's pinned transaction"),
      ])

      expect(assistantText(second)).toBe("beta")
      expect(Number(rows[0]?.n)).toBeGreaterThanOrEqual(2)
    })
  }, 120_000)
})
