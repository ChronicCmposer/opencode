/**
 * Anchor manifest (plan task 3.2).
 *
 * Each anchor names a *symbol* the plan's decisions rely upon — never a line
 * number. The manifest is data: {@link ANCHORS} lists `(module, symbol)` pairs
 * and {@link resolveAnchor} loads the module namespace at test time. If a
 * relied-upon export is renamed or removed, its anchor fails at the symbol,
 * which is the stable contract the plan depends on.
 *
 * `module: null` means the symbol lives on `globalThis` (platform primitives
 * proven in Phase 2).
 */

export interface Anchor {
  /** Stable identifier used in diagnostics. */
  readonly id: string
  /** Plan task or decision this anchor backs. */
  readonly plan: string
  /** Module specifier, or `null` for a `globalThis` member. */
  readonly module: string | null
  /** Exported symbol (or `globalThis` member) name. */
  readonly symbol: string
}

export const ANCHORS: readonly Anchor[] = [
  // Live V1 path the plan relocates (run-state.ts + effect/runner.ts).
  { id: "run-state.service", plan: "3.1 live V1 path", module: "@/session/run-state", symbol: "Service" },
  { id: "run-state.node", plan: "3.1 live V1 path", module: "@/session/run-state", symbol: "node" },
  { id: "runner.make", plan: "3.1 live V1 path", module: "@/effect/runner", symbol: "make" },
  { id: "runner.busy", plan: "5.4 RunnerBusy", module: "@/effect/runner", symbol: "Busy" },
  { id: "runner.cancelled", plan: "11.2 terminate cancel", module: "@/effect/runner", symbol: "Cancelled" },
  { id: "prompt.service", plan: "5.1 SessionPrompt", module: "@/session/prompt", symbol: "Service" },
  { id: "status.service", plan: "12.x SessionStatus", module: "@/session/status", symbol: "Service" },

  // V2 seam that must NOT be relocated.
  { id: "session-v2", plan: "1.6 V2 seam", module: "@opencode-ai/core/session", symbol: "SessionV2" },
  {
    id: "move-session",
    plan: "1.6 V2 seam",
    module: "@opencode-ai/core/control-plane/move-session",
    symbol: "MoveSession",
  },
  {
    id: "session-execution-local",
    plan: "1.6 V2 seam",
    module: "@opencode-ai/core/session/execution/local",
    symbol: "node",
  },

  // Single-writer persistence boundary.
  { id: "database.service", plan: "7.2 writer RPC", module: "@opencode-ai/core/database/database", symbol: "Service" },
  { id: "event.service", plan: "8.9 EventV2 bus", module: "@opencode-ai/core/event", symbol: "Service" },
  {
    id: "event.overflow",
    plan: "8.8 SubscriberOverflowError",
    module: "@opencode-ai/core/event",
    symbol: "SubscriberOverflowError",
  },
  {
    id: "event-sequence-table",
    plan: "7.10 event_sequence",
    module: "@opencode-ai/core/event/sql",
    symbol: "EventSequenceTable",
  },
  { id: "event-table", plan: "7.10 event rows", module: "@opencode-ai/core/event/sql", symbol: "EventTable" },
  {
    id: "projector.namespace",
    plan: "7.12 projectors",
    module: "@opencode-ai/core/session/projector",
    symbol: "SessionProjector",
  },
  { id: "projector.node", plan: "7.12 projectors", module: "@opencode-ai/core/session/projector", symbol: "node" },
  { id: "storage.service", plan: "7.13 Storage writes", module: "@/storage/storage", symbol: "Service" },

  // Egress / bus.
  { id: "global-bus", plan: "8.1 hub GlobalBus", module: "@/bus/global", symbol: "GlobalBus" },
  { id: "rpc.listen", plan: "40/41 util/rpc frames", module: "@/util/rpc", symbol: "listen" },
  { id: "rpc.emit", plan: "40/41 util/rpc frames", module: "@/util/rpc", symbol: "emit" },
  { id: "rpc.client", plan: "40/41 util/rpc frames", module: "@/util/rpc", symbol: "client" },

  // Isolation / runtime.
  { id: "runtime-flags.service", plan: "14.1 rollout flag", module: "@/effect/runtime-flags", symbol: "Service" },
  { id: "runtime-flags.node", plan: "14.1 rollout flag", module: "@/effect/runtime-flags", symbol: "node" },
  { id: "instance-state.make", plan: "6.8 InstanceState", module: "@/effect/instance-state", symbol: "make" },
  { id: "instance-state.get", plan: "6.8 InstanceState", module: "@/effect/instance-state", symbol: "get" },
  { id: "app-layer", plan: "6.1 reduced AppLayer", module: "@/effect/app-runtime", symbol: "AppLayer" },
  { id: "background-job.service", plan: "9.2 BackgroundJob client", module: "@/background/job", symbol: "Service" },

  // Transport / platform primitives proven in Phase 2.
  { id: "worker", plan: "2.1/4.x Bun Worker", module: null, symbol: "Worker" },
  { id: "message-port", plan: "8.1 MessagePort egress", module: null, symbol: "MessagePort" },
  { id: "response", plan: "2.2 non-transferable", module: null, symbol: "Response" },
  { id: "readable-stream", plan: "2.2 non-transferable", module: null, symbol: "ReadableStream" },
  { id: "abort-signal", plan: "2.2 non-transferable", module: null, symbol: "AbortSignal" },
  { id: "available-parallelism", plan: "4.2 K sizing", module: "node:os", symbol: "availableParallelism" },

  // Effect primitives the architecture relies upon.
  { id: "effect.deferred", plan: "11.9 cancel awaits", module: "effect", symbol: "Deferred" },
  { id: "effect.synchronized-ref", plan: "5.2 run-state", module: "effect", symbol: "SynchronizedRef" },
  { id: "effect.semaphore", plan: "7.x writer serialization", module: "effect", symbol: "Semaphore" },
  { id: "effect.latch", plan: "5.4 shell readiness", module: "effect", symbol: "Latch" },
  { id: "effect.queue", plan: "8.8 Queue.sliding", module: "effect", symbol: "Queue" },
]

/** Resolves an anchor to its runtime value: `undefined` means it is absent. */
export async function resolveAnchor(anchor: Anchor): Promise<unknown> {
  if (anchor.module === null) return (globalThis as Record<string, unknown>)[anchor.symbol]
  const namespace: Record<string, unknown> = await import(anchor.module)
  return namespace[anchor.symbol]
}
