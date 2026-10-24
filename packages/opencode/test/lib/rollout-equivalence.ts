// Phase 14.2 — rollout golden-equivalence harness.
//
// Proves the rollout claim: relocating a session's execution into a worker isolate
// does not change its durable output. One mock-SSE session is run two ways —
//   * the HUB path (rollout flag OFF): the session runs in-process on the hub's
//     portable `SessionRunMachine`, the machine the hub keeps when workers are off;
//   * the WORKER path (rollout flag ON): the session runs in a real Bun worker
//     isolate through the landed executor pool (`runParallelSessions`).
// — and each run's chat result is projected to the session's durable transcript.
//
// The transcript is the SPEC: a deterministic mapping from the observable chat
// result to the durable rows a session commits. Because both paths consume the
// same mock SSE stream, an identical transcript is exactly the property the
// rollout must preserve. Rows are recorded through the 3.12 golden helper, which
// stores the FULL row `{aggregate_id, seq, type, data, owner_id}`.

import { Effect, Exit, Scope } from "effect"
import type { SessionID } from "@/session/schema"
import { createSessionRunMachine, type SessionExecutionResult } from "@/session/worker/execute"
import { createGoldenCapture, type GoldenCapture, type GoldenRow } from "./golden"
import { DEFAULT_BURN_ITERATIONS, burnCpu, consumeMockSse } from "./mock-sse-worker"
import { runParallelSessions } from "./session-parallelism"

/** The default per-run owner fencing every durable row in a rollout capture. */
export const DEFAULT_OWNER = "run-rollout"

/**
 * The observable chat result both paths produce from the same mock SSE stream.
 * `hash` is the deterministic CPU-burn fingerprint (see `burnCpu`): it proves
 * each path actually performed the same synchronous work, so it is parsed and
 * validated rather than discarded.
 */
export interface SessionChatResult {
  readonly text: string
  readonly chunks: number
  readonly hash: number
}

/**
 * The SPEC inputs: the observable chat content without the host-dependent burn
 * fingerprint. The durable transcript is a pure function of this content, so the
 * golden spec stays stable across burn budgets.
 */
export type SessionSpecChat = Omit<SessionChatResult, "hash">

/** A full durable row as the golden helper compares it (the canonical shape). */
export type SessionDurableRow = GoldenRow

// --- Boundary parsing (Parse, Don't Validate) -------------------------------

/** Parse an untrusted session id string into a trusted `SessionID`, failing loud. */
function requireSessionID(value: unknown): SessionID {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`session id must be a non-empty string (got ${JSON.stringify(value)})`)
  }
  return value as SessionID
}

/** Parse an untrusted worker result value into a trusted chat result, failing loud. */
function parseChatResult(result: SessionExecutionResult): SessionChatResult {
  const value = result.value
  if (value === null || typeof value !== "object") {
    throw new Error(`session result has no object value (got ${JSON.stringify(value)})`)
  }
  const record = value as Record<string, unknown>
  const text = record["text"]
  const chunks = record["chunks"]
  if (typeof text !== "string") {
    throw new Error(`session result text must be a string (got ${JSON.stringify(text)})`)
  }
  if (typeof chunks !== "number" || !Number.isInteger(chunks) || chunks < 0) {
    throw new Error(`session result chunks must be a non-negative integer (got ${JSON.stringify(chunks)})`)
  }
  const hash = record["hash"]
  if (typeof hash !== "number" || !Number.isInteger(hash) || hash < 0) {
    throw new Error(`session result hash must be a non-negative integer (got ${JSON.stringify(hash)})`)
  }
  return { text, chunks, hash }
}

/**
 * Parse the CPU-burn fingerprint out of a run's result. Fails loudly when the
 * value carries no valid hash, so a path that skipped the synchronous burn can
 * never be recorded as equivalent.
 */
export function sessionBurnHash(result: SessionExecutionResult): number {
  return parseChatResult(result).hash
}

// --- The durable transcript (the spec) --------------------------------------

/**
 * The durable rows a mock-SSE session commits, in commit order. All rows share the
 * session aggregate (the durable aggregate is the session id — `session/worker/layer.ts`),
 * so `seq` is contiguous from 0. Pure: same chat result ⇒ same rows.
 */
export function projectSessionDurable(
  sessionID: SessionID,
  chat: SessionSpecChat,
  ownerID: string = DEFAULT_OWNER,
): ReadonlyArray<SessionDurableRow> {
  const events: ReadonlyArray<{ readonly type: string; readonly data: unknown }> = [
    { type: "session.created", data: { sessionID } },
    { type: "message.assistant", data: { text: chat.text } },
    { type: "session.idle", data: { sessionID, chunks: chat.chunks } },
  ]
  return events.map((event, seq) => ({
    aggregate_id: sessionID,
    seq,
    type: event.type,
    data: event.data,
    owner_id: ownerID,
  }))
}

/** Record one run's full durable transcript onto a golden capture. */
export function recordSessionDurable(
  golden: GoldenCapture,
  result: SessionExecutionResult,
  ownerID: string = DEFAULT_OWNER,
): void {
  const sessionID = requireSessionID(result.sessionID)
  const chat = parseChatResult(result)
  for (const row of projectSessionDurable(sessionID, chat, ownerID)) golden.durable(row)
}

/** The golden capture of the SPEC transcript for a known chat result. */
export function specCapture(
  sessionID: SessionID,
  chat: SessionSpecChat,
  ownerID: string = DEFAULT_OWNER,
): GoldenCapture {
  const golden = createGoldenCapture()
  for (const row of projectSessionDurable(sessionID, chat, ownerID)) golden.durable(row)
  return golden
}

// --- The two execution paths ------------------------------------------------

export interface HubSessionOptions {
  /** Base URL of the mock provider (tests: `TestLLMServer.url`). */
  readonly baseUrl: string
  readonly sessionID: SessionID
  readonly burnIterations?: number
  readonly model?: string
  readonly apiKey?: string
}

/**
 * Flag-OFF (hub) path: run the session in-process through the hub's portable
 * `SessionRunMachine`. The work consumes the same mock SSE stream and burns the
 * same CPU as the worker fixture, so the observable result is comparable.
 */
export async function runHubSession(options: HubSessionOptions): Promise<SessionExecutionResult> {
  const burnIterations = options.burnIterations ?? DEFAULT_BURN_ITERATIONS
  const bootstrap = {
    baseUrl: options.baseUrl,
    model: options.model ?? "mock-model",
    apiKey: options.apiKey ?? "test-key",
  }
  const scope = await Effect.runPromise(Scope.make())
  try {
    const machine = createSessionRunMachine({ scope })
    const work = Effect.gen(function* () {
      const parsed = yield* Effect.promise(() =>
        consumeMockSse(bootstrap, [{ role: "user", content: `session ${options.sessionID}` }]),
      )
      const hash = burnCpu(burnIterations, parsed.text.length)
      return {
        sessionID: options.sessionID,
        value: { text: parsed.text, chunks: parsed.chunks, hash },
      } satisfies SessionExecutionResult
    })
    return await Effect.runPromise(machine.ensureRunning(options.sessionID, work))
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void)).catch(() => undefined)
  }
}

export interface WorkerSessionOptions {
  /** Base URL of the mock provider (tests: `TestLLMServer.url`). */
  readonly baseUrl: string
  readonly sessionID: SessionID
  readonly burnIterations?: number
  readonly cores?: number
  readonly timeoutMs?: number
}

/**
 * Flag-ON (worker) path: run the session in a real Bun worker isolate through the
 * landed executor pool, then return its single result. One worker, one session.
 *
 * The pool labels its internal sessions (`ses_par_*`); the caller's id is the
 * identity under test. A worker result's `value` never embeds the id (it carries
 * the observable chat output only), so re-keying the result to the caller's id is
 * exact — the durable transcript is unchanged.
 */
export async function runWorkerSession(options: WorkerSessionOptions): Promise<SessionExecutionResult> {
  const report = await runParallelSessions({
    sessions: 1,
    poolSize: 1,
    baseUrl: options.baseUrl,
    burnIterations: options.burnIterations ?? DEFAULT_BURN_ITERATIONS,
    ...(options.cores === undefined ? {} : { cores: options.cores }),
    ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
  })
  const [result] = report.results
  if (result === undefined) throw new Error("worker path produced no session result")
  return { ...result, sessionID: options.sessionID }
}
