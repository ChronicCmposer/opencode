// Phase 4.1 — production session-worker bootstrap (plan task 4.1).
//
// This is the REAL worker entry point the pool spawns, distinct from the test
// fixture `test/fixture/effect-worker.ts`. It boots an Effect runtime inside a
// Bun Worker isolate and answers a minimal JSON request/response protocol over
// the default worker channel:
//
//   on load                            -> out: { type: "ready", threadId }
//   in : { type: "run", id, value }    -> out: { type: "value", id, value }
//   in : { type: "fail", id, message } -> out: { type: "error", id, message }
//   anything else                      -> out: { type: "error", message }
//
// The protocol is intentionally trivial; later phases replace `run` with real
// session dispatch while keeping the ready + request/response transport.

import { threadId } from "node:worker_threads"
import { Effect } from "effect"

/** A trivial Effect: doubles `value` across an async boundary. */
export const runTrivial = (value: number): Effect.Effect<number> =>
  Effect.gen(function* () {
    const doubled = yield* Effect.promise(async () => value * 2)
    return doubled
  })

export interface BootstrapReady {
  readonly type: "ready"
  readonly threadId: number
}

export interface BootstrapValue {
  readonly type: "value"
  readonly id: number
  readonly value: number
}

export interface BootstrapError {
  readonly type: "error"
  readonly id?: number
  readonly message: string
}

export type BootstrapOutbound = BootstrapReady | BootstrapValue | BootstrapError

export type BootstrapInbound =
  | { readonly type: "run"; readonly id: number; readonly value: number }
  | { readonly type: "fail"; readonly id: number; readonly message: string }

interface WorkerScope {
  postMessage(message: BootstrapOutbound): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as WorkerScope

const isRunMessage = (message: Partial<BootstrapInbound>): message is Extract<BootstrapInbound, { type: "run" }> =>
  message.type === "run" && typeof message.id === "number" && typeof message.value === "number"

const isFailMessage = (message: Partial<BootstrapInbound>): message is Extract<BootstrapInbound, { type: "fail" }> =>
  message.type === "fail" && typeof message.id === "number" && typeof message.message === "string"

const toError = (id: number | undefined, reason: unknown): BootstrapError => ({
  type: "error",
  ...(id === undefined ? {} : { id }),
  message: reason instanceof Error ? reason.message : String(reason),
})

const handleRun = (id: number, value: number): void => {
  void runTrivial(value)
    .pipe(
      Effect.match({
        onFailure: (reason) => toError(id, reason),
        onSuccess: (result) => ({ type: "value", id, value: result }) as const,
      }),
      Effect.runPromise,
    )
    .then(
      (message) => scope.postMessage(message),
      // A defect (not a typed failure) escapes `Effect.match`; still answer with
      // a structured error frame instead of rejecting silently.
      (reason) => scope.postMessage(toError(id, reason)),
    )
}

const handleFail = (id: number, message: string): void => {
  void Effect.fail(message)
    .pipe(
      Effect.match({
        onFailure: (reason) => toError(id, reason),
        onSuccess: () => toError(id, "unexpected success"),
      }),
      Effect.runPromise,
    )
    .then(
      (out) => scope.postMessage(out),
      (reason) => scope.postMessage(toError(id, reason)),
    )
}

scope.onmessage = (event) => {
  const message = (event.data ?? {}) as Partial<BootstrapInbound>

  if (isRunMessage(message)) {
    handleRun(message.id, message.value)
    return
  }

  if (isFailMessage(message)) {
    handleFail(message.id, message.message)
    return
  }

  scope.postMessage(toError(undefined, `unknown message: ${String(message.type)}`))
}

scope.postMessage({ type: "ready", threadId })

// NOTE: no `export * as` self-reexport here — this module has top-level worker
// side effects and is meant to be loaded only as a Worker entry point. Import
// its types with `import type` (erased at runtime) from the hub side.
