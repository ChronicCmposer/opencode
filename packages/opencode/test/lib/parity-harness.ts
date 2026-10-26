// Single-side scenario runner over the writer-backed worker E2E context.
//
// The context itself (writer port, SQLite schema, hub-proxy authorities, worker
// layer) lives in `./e2e-context`; this module keeps the historical import path
// and adds `runScenario` plus the trace normalizer used by parity suites.

import { Effect, ManagedRuntime } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceRef } from "@/effect/instance-ref"
import type { WorkerModelConfig, WorkerSnapshot } from "@/session/worker/layer"
import { createE2EContext, type E2EContext } from "./e2e-context"

export {
  applyBaseSchema,
  createE2EContext,
  withTempDir,
  withWorkerE2E,
  type E2EContext,
  type E2EContextOptions,
  type E2EStatusRecord,
  type TempDirHandle,
  type WorkerE2E,
  type WithWorkerE2EOptions,
} from "./e2e-context"

// --- Comparable single-side scenario runner ---------------------------------
//
// `runScenario` runs one scenario through the worker E2E context above in the
// requested flag state, so a test can execute the SAME deterministic scenario
// twice and compare the normalized outcomes:
//
//   * `flagOn: true`  — hub-owned authorities (`SessionStatus`/`Permission`/
//     `Question`) are reached over the hub proxy, as a per-session worker does;
//   * `flagOn: false` — those authorities stay in the worker graph, the
//     in-process/flag-OFF parity wiring.
//
// The runner is deliberately single-sided: it never starts two runtimes itself.
// The caller owns the comparison and decides whether the two sides are allowed
// to differ (see `parity-flag.test.ts`).
//
// LIMITATION: this runner does not set `OPENCODE_EXPERIMENTAL_SESSION_WORKERS`
// or launch the production hub/worker-pool graph. It pins the *observable
// authority wiring* a per-session worker sees (hub proxy vs in-process), which
// is where the flag changes behavior inside one worker. The full env-flag,
// provider-driven session graph parity (events + durable rows across two real
// modes) lives in `./parity-session.ts` (`runParitySession`); use that when a
// scenario needs the real flag-off session loop. Callers that need the env flag
// itself must toggle it around spawning a separate process, not here.

/** What a scenario sees: the flag state and the fully wired worker context. */
export interface ScenarioContext {
  readonly flagOn: boolean
  readonly context: E2EContext
}

/** One deterministic scenario both flag states can run. */
export interface Scenario<A, R = never> {
  readonly name: string
  readonly snapshot: WorkerSnapshot
  readonly modelConfig: WorkerModelConfig
  readonly nodes?: ReadonlyArray<LayerNode.AnyNode>
  readonly replacements?: LayerNode.Replacements
  /** Runs against the worker runtime; `R` is the services the runtime provides. */
  readonly run: (context: ScenarioContext) => Effect.Effect<A, never, R>
}

/** The captured outcome of running one scenario in one flag state. */
export interface ScenarioTrace<A> {
  readonly name: string
  readonly flagOn: boolean
  readonly value: A
}

/** Run `scenario` once in the requested flag state, disposing the whole graph. */
export async function runScenario<A, R = never>(
  flagOn: boolean,
  scenario: Scenario<A, R>,
): Promise<ScenarioTrace<A>> {
  const context = createE2EContext({
    snapshot: scenario.snapshot,
    modelConfig: scenario.modelConfig,
    hubProxies: flagOn ? "local" : "in-process",
    ...(scenario.nodes === undefined ? {} : { nodes: scenario.nodes }),
    ...(scenario.replacements === undefined ? {} : { replacements: scenario.replacements }),
  })
  const runtime = ManagedRuntime.make(context.layer)
  try {
    const value = await runtime.runPromise(
      scenario.run({ flagOn, context }).pipe(Effect.provideService(InstanceRef, context.instance)),
    )
    return { name: scenario.name, flagOn, value }
  } finally {
    await runtime.dispose()
    await context.dispose()
  }
}

/** Generated identifier families whose concrete values differ per run. */
const TRACE_ID_PATTERNS: ReadonlyArray<readonly [RegExp, string]> = [
  [/\bses_[A-Za-z0-9]+/g, "ses_<id>"],
  [/\bmsg_[A-Za-z0-9]+/g, "msg_<id>"],
  [/\bprt_[A-Za-z0-9]+/g, "prt_<id>"],
  [/\bcall_[A-Za-z0-9]+/g, "call_<id>"],
  [/\bjob_[A-Za-z0-9]+/g, "job_<id>"],
  [/\bevt_[A-Za-z0-9]+/g, "evt_<id>"],
]

/** Fields carrying wall-clock values that differ between the two sides. */
const VOLATILE_TIME_KEYS: ReadonlySet<string> = new Set([
  "time",
  "created",
  "updated",
  "started_at",
  "completed_at",
])

/**
 * Project a scenario outcome to its comparable shape: generated ids collapse to
 * one placeholder and wall-clock fields become `0`, recursively. Non-string
 * leaves and deliberate enum values pass through unchanged, so a divergence in
 * behavior still surfaces.
 */
export function normalizeTrace(value: unknown): unknown {
  if (typeof value === "string") {
    return TRACE_ID_PATTERNS.reduce((text, [pattern, replacement]) => text.replace(pattern, replacement), value)
  }
  if (Array.isArray(value)) return value.map(normalizeTrace)
  if (value === null || typeof value !== "object") return value
  const record = value as Record<string, unknown>
  return Object.fromEntries(
    Object.entries(record).map(([key, entry]) => [
      key,
      VOLATILE_TIME_KEYS.has(key) && typeof entry === "number" ? 0 : normalizeTrace(entry),
    ]),
  )
}
