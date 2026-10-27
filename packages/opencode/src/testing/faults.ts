// Test-only fault-injection seams.
//
// WHY THIS EXISTS
//   Some failure modes the pool/writer must survive are unreachable on demand:
//   a sole SQLite connection never returns `SQLITE_BUSY`, and a worker only
//   crashes when it crashes. This module is the single, explicit seam that lets
//   tests arm those failures deterministically. It is inert unless the gate is
//   set, so the production path pays one cached env read per call.
//
// CONTRACT (the whole surface is the three hooks + the parsed plan)
//
//   Gate   : environment variable `OPENCODE_TEST_FAULTS`.
//   Spec   : comma-separated directives, each `kind@point` (whitespace around
//            tokens is ignored). Unset/empty => disabled.
//   Point  : an opaque, call-site-chosen label (e.g. `worker:run`,
//            `writer:commit`, `writer:tx`). A directive only fires at its exact
//            point, so faults can be scoped to one step of a pipeline.
//   Kinds  : three, matching the plan's named symbols.
//
//            | kind           | scenario(s) injected                              | hook result         |
//            |----------------|---------------------------------------------------|---------------------|
//            | `crashWorker`  | a worker crash; a *mid-commit* crash when pointed | hard-exits isolate  |
//            |                | at the writer's commit step                       | (never returns)     |
//            | `failCommitAt` | forced `SQLITE_BUSY` on the commit step           | a `CommitFault`     |
//            | `abortTxAfter` | the requester dying mid-write                     | `true`: abandon tx  |
//
//   The reader maps the four required scenarios onto these kinds:
//     worker crash              -> `crashWorker@<worker point>`
//     forced `SQLITE_BUSY`      -> `failCommitAt@writer:commit`
//     mid-commit crash          -> `crashWorker@writer:commit` (durable publish)
//                                  `crashWorker@writer:tx-commit` (reserved-handle tx)
//     requester death mid-write -> `abortTxAfter@writer:tx`
//
//   A malformed or unknown directive is a test-configuration bug and fails
//   loudly at parse time (`FaultSpecError`) rather than being silently ignored.
//
//   "Compiled out of production": the hooks early-exit on a cached plan; a
//   future build-time `define` may inline `activeFaults()` to the disabled plan.
//
// USAGE AT A CALL SITE
//   import { Faults } from "@/testing/faults"
//
//   yield* Effect.sync(() => Faults.crashWorker("writer:commit"))       // may hard-exit
//   const busy = Faults.failCommitAt("writer:commit")                   // | undefined
//   if (busy) return yield* Effect.fail(busy)
//   if (Faults.abortTxAfter("writer:tx")) return rollbackPinnedTx()

import { Schema } from "effect"

/** Environment variable that gates every hook. */
export const FAULTS_ENV = "OPENCODE_TEST_FAULTS"

/** Exit code used by the default `crashWorker` action (worker crash = exit/`onerror`). */
export const WORKER_CRASH_EXIT_CODE = 70

/** The three fault kinds, declared once; `FaultKind` is derived from this list. */
export const FAULT_KINDS = ["crashWorker", "failCommitAt", "abortTxAfter"] as const

export type FaultKind = (typeof FAULT_KINDS)[number]

const FAULT_KIND_SET: ReadonlySet<string> = new Set(FAULT_KINDS)

export interface FaultDirective {
  readonly kind: FaultKind
  readonly point: string
}

/** A parsed, trusted fault plan. `has` is the only membership query. */
export interface FaultPlan {
  readonly enabled: boolean
  readonly directives: ReadonlyArray<FaultDirective>
  has(kind: FaultKind, point: string): boolean
}

/** Raised when `OPENCODE_TEST_FAULTS` cannot be parsed. Loud on purpose. */
export class FaultSpecError extends Schema.TaggedErrorClass<FaultSpecError>()("FaultSpecError", {
  message: Schema.String,
  directive: Schema.optional(Schema.String),
}) {}

/** The forced `SQLITE_BUSY` returned by `failCommitAt`. */
export class CommitFault extends Schema.TaggedErrorClass<CommitFault>()("CommitFault", {
  point: Schema.String,
  sqliteCode: Schema.Literal("SQLITE_BUSY"),
  message: Schema.String,
}) {}

const DISABLED_PLAN: FaultPlan = Object.freeze({
  enabled: false,
  directives: Object.freeze([]) as ReadonlyArray<FaultDirective>,
  has: () => false,
})

/**
 * Parse a raw `OPENCODE_TEST_FAULTS` value into a trusted plan.
 * Pure: the same string always yields the same plan.
 */
export function parseFaultPlan(raw: string | undefined): FaultPlan {
  const spec = raw?.trim()
  if (!spec) return DISABLED_PLAN

  const directives: FaultDirective[] = []
  for (const token of spec
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)) {
    const separator = token.indexOf("@")
    if (separator <= 0 || separator === token.length - 1) {
      throw new FaultSpecError({
        message: `malformed ${FAULTS_ENV} directive "${token}": expected "kind@point"`,
        directive: token,
      })
    }
    const kind = token.slice(0, separator).trim()
    if (!FAULT_KIND_SET.has(kind)) {
      throw new FaultSpecError({
        message: `unknown ${FAULTS_ENV} kind "${kind}": expected one of ${FAULT_KINDS.join(", ")}`,
        directive: token,
      })
    }
    directives.push({ kind: kind as FaultKind, point: token.slice(separator + 1).trim() })
  }

  if (directives.length === 0) return DISABLED_PLAN
  const frozen = Object.freeze(directives) as ReadonlyArray<FaultDirective>
  return Object.freeze({
    enabled: true,
    directives: frozen,
    has: (kind: FaultKind, point: string) => frozen.some((d) => d.kind === kind && d.point === point),
  })
}

/**
 * A single-entry memo keyed by the raw gate value. Parsing happens before the
 * cache is touched, so a malformed spec throws without poisoning later reads.
 */
function createPlanCache() {
  let cachedRaw: string | undefined
  let cachedPlan: FaultPlan | undefined
  return {
    read(raw: string | undefined): FaultPlan {
      if (cachedPlan !== undefined && cachedRaw === raw) return cachedPlan
      const plan = parseFaultPlan(raw)
      cachedRaw = raw
      cachedPlan = plan
      return plan
    },
    reset(): void {
      cachedRaw = undefined
      cachedPlan = undefined
    },
  }
}

const planCache = createPlanCache()

/** Clear the memoized plan (test seam for the env-gate cache). */
export function resetFaultsCache(): void {
  planCache.reset()
}

/**
 * Build-time gate for the whole seam. `script/build.ts` inlines
 * `OPENCODE_TEST_FAULTS_ENABLED: false` into the production bundle, so every hook
 * short-circuits before it ever touches the environment. In dev/test the symbol is
 * absent, so the `OPENCODE_TEST_FAULTS` env gate is honoured exactly as before.
 */
declare const OPENCODE_TEST_FAULTS_ENABLED: boolean | undefined

/**
 * True when the build compiled the seam out. `typeof` guards the bare identifier
 * so an absent define (dev/test) is `undefined` rather than a `ReferenceError`.
 */
function compiledOut(): boolean {
  return typeof OPENCODE_TEST_FAULTS_ENABLED !== "undefined" && OPENCODE_TEST_FAULTS_ENABLED === false
}

/**
 * The active plan, read from the environment at call time (cached per value so
 * tests may change the gate between cases without a stale plan lingering). Once
 * the build compiles the seam out this is unconditionally the disabled plan.
 */
export function activeFaults(): FaultPlan {
  if (compiledOut()) return DISABLED_PLAN
  return planCache.read(process.env[FAULTS_ENV])
}

export interface CrashOptions {
  /** Action invoked instead of the default exit; must not return. */
  readonly onCrash?: (directive: FaultDirective) => never
}

/**
 * The three fault hooks behind one indirection, so the production build can swap
 * in a no-op port (or an Effect `Layer.empty`) without each call site changing.
 */
export interface FaultPort {
  crashWorker(point: string, options?: CrashOptions): void
  failCommitAt(point: string): CommitFault | undefined
  abortTxAfter(point: string): boolean
}

function exitIsolate(): never {
  process.exit(WORKER_CRASH_EXIT_CODE)
}

/** The real hooks: env-gated, inert unless armed. */
const liveFaultPort: FaultPort = {
  crashWorker(point, options = {}) {
    const faults = activeFaults()
    if (!faults.enabled || !faults.has("crashWorker", point)) return
    ;(options.onCrash ?? exitIsolate)({ kind: "crashWorker", point })
  },
  failCommitAt(point) {
    const faults = activeFaults()
    if (!faults.enabled || !faults.has("failCommitAt", point)) return undefined
    return new CommitFault({ point, sqliteCode: "SQLITE_BUSY", message: `forced SQLITE_BUSY at ${point}` })
  },
  abortTxAfter(point) {
    const faults = activeFaults()
    return faults.enabled && faults.has("abortTxAfter", point)
  },
}

/** The production port: every hook is a no-op, with no environment read. */
const emptyFaultPort: FaultPort = {
  crashWorker: () => {},
  failCommitAt: () => undefined,
  abortTxAfter: () => false,
}

/** The selected port: the no-op one once the build compiles the seam out. */
const faultPort: FaultPort = compiledOut() ? emptyFaultPort : liveFaultPort

/**
 * Hard-crash the current isolate when `crashWorker@point` is armed. No-op else.
 * `onCrash` is the injection seam used by tests to observe the crash without
 * killing the test runner.
 */
export function crashWorker(point: string, options: CrashOptions = {}): void {
  faultPort.crashWorker(point, options)
}

/**
 * Return a forced `SQLITE_BUSY` when `failCommitAt@point` is armed, else
 * `undefined`. The caller turns the fault into its own typed failure channel.
 */
export function failCommitAt(point: string): CommitFault | undefined {
  return faultPort.failCommitAt(point)
}

/**
 * `true` when `abortTxAfter@point` is armed: the writer must abandon the pinned
 * transaction mid-write (requester death), as if the requester's port closed.
 */
export function abortTxAfter(point: string): boolean {
  return faultPort.abortTxAfter(point)
}

export * as Faults from "./faults"
