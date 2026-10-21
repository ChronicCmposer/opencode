// Step 1 — `SessionWorkerInit` builder for session-worker tests.
//
// The production entry (`src/session/worker/session-worker.ts`) parses a
// `{snapshot, env, model}` payload out of `workerData` and fails loud on a
// malformed one. Every test that boots that entry needs the same trusted shape,
// so this module is the single builder: defaults are dependency-free and every
// override is layered over them.
//
// `assertStructuredCloneable` proves a payload survives the isolate boundary
// (workerData is structured-cloned, never a closure): it clones and compares,
// failing loud with a descriptive error otherwise.

import { isDeepStrictEqual } from "node:util"
import type { SessionWorkerInit, SessionWorkerModel } from "@/session/worker/session-worker"
import type { WorkerEnv, WorkerSnapshot } from "@/session/worker/layer"
import { workerSnapshot } from "./snapshot"

/** The mock-provider model bootstrap a worker consumes (`session-worker.ts:34-38`). */
export const TEST_WORKER_MODEL: SessionWorkerModel = {
  baseUrl: "http://127.0.0.1:0/v1",
  apiKey: "test-key",
  model: "test-model",
}

/** Per-field overrides; anything omitted falls back to the dependency-free default. */
export interface SessionWorkerInitOverrides {
  readonly snapshot?: WorkerSnapshot
  readonly env?: WorkerEnv
  readonly model?: Partial<SessionWorkerModel>
}

/**
 * Build a `SessionWorkerInit`. The default snapshot comes from `workerSnapshot()`,
 * the default env is empty, and the model defaults to `TEST_WORKER_MODEL` with any
 * supplied fields merged on top.
 */
export function sessionWorkerInit(overrides: SessionWorkerInitOverrides = {}): SessionWorkerInit {
  return {
    snapshot: overrides.snapshot ?? workerSnapshot(),
    env: overrides.env ?? { values: {} },
    model: { ...TEST_WORKER_MODEL, ...overrides.model },
  }
}

/**
 * Assert `value` survives a `structuredClone` round-trip unchanged and return the
 * clone. Fails loud (Law of "Fail Fast, Fail Loud") when the value is not
 * cloneable or when the clone differs, instead of letting a broken payload reach
 * the isolate boundary.
 */
export function assertStructuredCloneable<T>(value: T): T {
  let cloned: T
  try {
    cloned = structuredClone(value)
  } catch (error) {
    throw new Error(`value did not survive structuredClone: ${error instanceof Error ? error.message : String(error)}`)
  }
  if (!isDeepStrictEqual(cloned, value)) {
    throw new Error("value did not survive structuredClone unchanged")
  }
  return cloned
}
