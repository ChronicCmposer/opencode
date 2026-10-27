// Parameterized probe worker for the small worker-isolate fixtures.
//
// Each probe is a self-contained script that answers one question about the
// isolate boundary (process role, birth thread name, boot cost, clock control,
// Effect + `@/` alias resolution, fault injection). They used to be six
// near-identical entry files; the probe body lives here once, and the legacy
// entries (`boot-worker.ts`, `effect-worker.ts`, …) stay as thin launchers so
// existing spawn paths and tests keep working.
//
// Selection: a launcher calls `runProbe(name)`, or an entry spawned directly
// sets `workerData.probe` / `OPENCODE_TEST_PROBE`. Heavy dependencies are
// imported inside each probe so the `boot` probe stays dependency-free.

import { workerData } from "node:worker_threads"
import type { ClockWorkerInbound } from "../lib/worker-clock"
import { onMessage, postToParent } from "./worker-scope"

export type ProbeName = "process-role" | "no-name" | "boot" | "clock" | "effect" | "fault"

const PROBES: ReadonlyArray<ProbeName> = ["process-role", "no-name", "boot", "clock", "effect", "fault"]

/** Parse an untyped probe selector, returning `undefined` when absent. */
export function probeNameOf(value: unknown): ProbeName | undefined {
  if (value === undefined || value === null || value === "") return undefined
  if (typeof value !== "string" || !PROBES.includes(value as ProbeName)) {
    throw new Error(`unknown probe ${String(value)}; expected one of ${PROBES.join(", ")}`)
  }
  return value as ProbeName
}

export interface EffectWorkerReady {
  readonly type: "ready"
  readonly threadId: number
}

export interface EffectWorkerComputed {
  readonly type: "computed"
  readonly value: number
  readonly alias: string
  readonly marker: string
}

export interface EffectWorkerFailed {
  readonly type: "failed"
  readonly message: string
}

export type EffectWorkerOutbound = EffectWorkerReady | EffectWorkerComputed | EffectWorkerFailed

export type EffectWorkerInbound =
  | { readonly type: "compute"; readonly value: number }
  | { readonly type: "fail"; readonly message: string }

/**
 * Observe the default process role, flip it once, and report the graph decision
 * before/after. Runs in its own isolate so `@/effect/process-role` module state
 * is fresh; the probe proves set-once semantics without leaking into the suite.
 */
async function runProcessRole(): Promise<void> {
  const { Layer } = await import("effect")
  const { LayerNode } = await import("@opencode-ai/core/effect/layer-node")
  const { processRole, setProcessRole } = await import("@/effect/process-role")
  const { sessionWorkerNodes } = await import("@/session/worker/nodes")

  const hubNode = LayerNode.make({
    name: "@opencode/SessionHub",
    layer: Layer.empty as import("effect").Layer.Layer<never>,
    deps: [] as const,
  })

  const capture = (fn: () => void): string | null => {
    try {
      fn()
      return null
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    }
  }

  const defaultRole = processRole()
  const nodesBefore = sessionWorkerNodes({ hubNode, enabled: true }).length

  setProcessRole("session")
  const roleAfter = processRole()
  const nodesAfter = sessionWorkerNodes({ hubNode, enabled: true }).length

  // A conflicting re-set must fail loud; re-setting the same role is a no-op.
  const conflict = capture(() => setProcessRole("hub"))
  const sameRole = capture(() => setProcessRole("session"))

  postToParent({ type: "ready" })
  postToParent({ type: "result", defaultRole, nodesBefore, roleAfter, nodesAfter, conflict, sameRole })
}

/**
 * Deliberately never calls `setThreadName`: proves `comm` comes from
 * `WorkerOptions.name` at creation. Posts the executor `ready` frame, then idles
 * so the test can read the OS thread name; teardown is the only stop.
 */
async function runNoName(): Promise<void> {
  // The production transport verifies the ready-frame build hash, so this
  // no-name probe still sends a valid handshake (it only avoids self-naming).
  const { readyFrame } = await import("@/session/worker/build-hash")
  postToParent(readyFrame(0))
  setInterval(() => {}, 1 << 30)
}

/** Dependency-free boot-cost probe: readiness plus the process-wide RSS sample. */
async function runBoot(): Promise<void> {
  const { threadId } = await import("node:worker_threads")
  postToParent({
    type: "ready",
    workerId: threadId,
    rssBytes: process.memoryUsage().rss,
  })
}

/**
 * Control-channel clock probe: transfers the worker end of a `MessageChannel`
 * and runs real `Effect.sleep`s on a frame-driven clock, replying when each
 * sleep resolves with the clock's current time.
 */
async function runClock(): Promise<void> {
  const { Effect } = await import("effect")
  const { attachClock } = await import("../lib/worker-clock")

  const messageOf = (error: unknown): string => (error instanceof Error ? error.message : String(error))

  const channel = new MessageChannel()
  const clock = attachClock(channel.port1)

  onMessage<ClockWorkerInbound>((message) => {
    if (!message || message.type !== "sleep") return
    Effect.gen(function* () {
      yield* Effect.sleep(message.ms)
      postToParent({ type: "slept", id: message.id, at: clock.currentTimeMillis() })
    })
      .pipe(Effect.provide(clock.layer), Effect.runPromise)
      .catch((error) => postToParent({ type: "error", id: message.id, message: messageOf(error) }))
  })

  postToParent(channel.port2, [channel.port2])
}

/**
 * Bootstrap probe: run an Effect across an async boundary and import two `src`
 * modules through the `@/` alias. Protocol:
 *   startup -> {type:"ready",threadId}
 *   {type:"compute",value} -> {type:"computed",value,alias,marker}
 *   {type:"fail",message} -> {type:"failed",message}
 */
async function runEffect(): Promise<void> {
  const { threadId } = await import("node:worker_threads")
  const { Effect } = await import("effect")
  const { decodeDataUrl } = await import("@/util/data-url")
  const { iife } = await import("@/util/iife")

  const ALIAS_SENTINEL = "alias-resolved"
  const ALIAS_DATA_URL = `data:text/plain;base64,${Buffer.from(ALIAS_SENTINEL).toString("base64")}`

  const compute = (value: number) =>
    Effect.gen(function* () {
      const doubled = yield* Effect.promise(async () => value * 2)
      const alias = yield* Effect.sync(() => decodeDataUrl(ALIAS_DATA_URL))
      const marker = yield* Effect.sync(() => iife(() => "iife-ok"))
      return { value: doubled, alias, marker }
    })

  const runCompute = (value: number): Promise<EffectWorkerOutbound> =>
    compute(value).pipe(
      Effect.map((result) => ({ type: "computed", ...result }) as const),
      Effect.runPromise,
    )

  const runFail = (message: string): Promise<EffectWorkerOutbound> =>
    Effect.fail(message).pipe(
      Effect.match({
        onFailure: (reason) => ({ type: "failed", message: reason }) as const,
        onSuccess: () => ({ type: "failed", message: "unexpected success" }) as const,
      }),
      Effect.runPromise,
    )

  onMessage<Partial<EffectWorkerInbound>>((raw) => {
    const message: Partial<EffectWorkerInbound> = raw ?? {}

    if (message.type === "compute") {
      if (typeof message.value !== "number") {
        postToParent({ type: "failed", message: `compute requires a numeric value, received ${typeof message.value}` })
        return
      }
      void runCompute(message.value).then((out) => postToParent(out))
      return
    }

    if (message.type === "fail") {
      void runFail(message.message ?? "worker failure").then((out) => postToParent(out))
      return
    }

    postToParent({ type: "failed", message: `unknown message: ${String(message.type)}` })
  })

  postToParent({ type: "ready", threadId })
}

/**
 * Fault-seam probe: crash at a named point when the gate is armed, otherwise
 * report `survived`. Proves `OPENCODE_TEST_FAULTS` crosses the isolate boundary.
 */
async function runFault(): Promise<void> {
  const { Faults } = await import("../../src/testing/faults")

  onMessage<{ type?: string; point?: string }>((raw) => {
    const input = raw ?? {}
    if (input.type !== "crash") return

    const point = input.point ?? ""
    Faults.crashWorker(point, {
      onCrash: (directive) => {
        postToParent({ type: "crashing", point: directive.point })
        process.exit(Faults.WORKER_CRASH_EXIT_CODE)
      },
    })
    postToParent({ type: "survived", point })
  })

  postToParent({ type: "ready" })
}

/** Execute one named probe in this isolate. */
export async function runProbe(probe: ProbeName): Promise<void> {
  switch (probe) {
    case "process-role":
      return runProcessRole()
    case "no-name":
      return runNoName()
    case "boot":
      return runBoot()
    case "clock":
      return runClock()
    case "effect":
      return runEffect()
    case "fault":
      return runFault()
  }
}

// Direct-entry mode: `workerData.probe` or `OPENCODE_TEST_PROBE` selects the
// probe. Launchers call `runProbe` themselves and therefore leave this unset.
const requested = probeNameOf((workerData as { probe?: unknown } | undefined)?.probe ?? process.env["OPENCODE_TEST_PROBE"])
if (requested !== undefined) await runProbe(requested)
