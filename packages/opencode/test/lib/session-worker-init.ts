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
import type { Role } from "@/session/worker/node-table"
import { workerSnapshot } from "./snapshot"

/**
 * The mock-provider model bootstrap a worker consumes. It carries the full
 * serialized descriptor (identity, transport, capabilities, limit); the
 * legacy runtime mapping for the old `{baseUrl, apiKey, model}` bootstrap
 * derived `providerID`/`apiId` from the model name, so the same identity is
 * pinned here to keep boot behaviour unchanged.
 */
export const TEST_WORKER_MODEL: SessionWorkerModel = {
  providerID: "test-model",
  modelID: "test-model",
  apiId: "test-model",
  baseUrl: "http://127.0.0.1:0/v1",
  apiKey: "test-key",
  capabilities: { temperature: true, reasoning: false },
  limit: { context: 128_000, output: 4_096 },
}

/** Per-field overrides; anything omitted falls back to the dependency-free default. */
export interface SessionWorkerInitOverrides {
  readonly snapshot?: WorkerSnapshot
  readonly env?: WorkerEnv
  /**
   * Descriptor fields merged over {@link TEST_WORKER_MODEL}. The legacy `model`
   * name is still accepted for backward compatibility and normalized to
   * `modelID`; an explicit `modelID` always wins.
   */
  readonly model?: Partial<SessionWorkerModel> & { readonly model?: string }
  /** Process role the worker adopts; defaults to `"session"`. */
  readonly role?: Role
}

/**
 * Build a `SessionWorkerInit`. The default snapshot comes from `workerSnapshot()`,
 * the default env is empty, and the model defaults to `TEST_WORKER_MODEL` with any
 * supplied fields merged on top.
 */
export function sessionWorkerInit(overrides: SessionWorkerInitOverrides = {}): SessionWorkerInit {
  const { model: legacyModelID, ...descriptor } = overrides.model ?? {}
  return {
    snapshot: overrides.snapshot ?? workerSnapshot(),
    env: overrides.env ?? { values: {} },
    role: overrides.role ?? "session",
    // Normalize the legacy `model` name at this boundary so the emitted payload
    // always carries the entry's `modelID` (which the entry's `modelConfig` reads
    // as `modelID ?? model`), keeping a legacy override effective rather than inert.
    model: {
      ...TEST_WORKER_MODEL,
      ...(legacyModelID === undefined ? {} : { modelID: legacyModelID }),
      ...descriptor,
    },
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
