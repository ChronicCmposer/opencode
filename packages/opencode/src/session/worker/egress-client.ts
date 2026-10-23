// W2 — worker-side egress client over the events envelope (spec Q6/Q7/Q21).
//
// The reduced session-worker graph talks to the hub event channel through a
// dedicated, transferred `MessagePort` (plan decisions 40/45). This module is the
// adapter that turns the worker's EventV2 events into framed bytes on the wire:
//
//   * the DATA plane is `createEgressSender` (`egress/sender.ts`) — coalescing
//     non-durable deltas and length-prefixed, versioned frames posted with a
//     transferred buffer (zero-copy);
//   * the CONTROL plane is `createMeshWorkerClient` (`egress/mesh.ts`) over a
//     SEPARATE port, so durable wakeups and resync never queue behind deltas.
//
// Events cross VERBATIM as `{ id, type, properties }` — there is no `data`
// translation. `emit` maps to a coalesced non-durable delta; `emitDurable`
// forwards an explicit writer-stamped `meta` and bypasses coalescing.

import type { EventV2Event } from "@/session/worker/frame"

import type { CoalescerScheduler } from "@/session/egress/coalesce"
import type { DurableMeta } from "@/session/egress/frame"
import { createMeshWorkerClient, type MeshWorkerClient } from "@/session/egress/mesh"
import { createEgressSender, type EgressSender } from "@/session/egress/sender"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { messageOf } from "./port"

export interface WorkerEgressClientOptions {
  /** Writer generation used by the durable ordinal fallback; passed to the sender. */
  readonly generation?: number
  /** Delta coalescing window in ms; passed to the sender. */
  readonly windowMs?: number
  /** Eager delta flush threshold; passed to the sender. */
  readonly maxBatch?: number
  /** Injectable coalescer timer, so the window is deterministic in tests. */
  readonly scheduler?: CoalescerScheduler
  /**
   * The worker's dedicated EventV2 mesh control port. When omitted the client
   * has no control plane (`mesh === undefined`).
   */
  readonly controlPort?: MessagePort
  /** Optional structured logger seam; absent defaults to a no-op (flag-off behavior). */
  readonly log?: WorkerLogger
  /**
   * The worker's owner id, when the caller knows it. Included on the lifecycle
   * log lines so a hub-side reader can correlate the egress peer with its pool
   * slot. Omitted (never fabricated) when not supplied.
   */
  readonly workerID?: number
}

export interface WorkerEgressClient {
  /** Emit a non-durable delta (the shape `SessionPipeline.run` uses). */
  emit(event: EventV2Event): void
  /** Emit a durable boundary event; `meta` defaults to the sender's ordinal. */
  emitDurable(event: EventV2Event, meta?: DurableMeta): void
  /** Flush buffered deltas, then emit the `end` frame (idempotent). */
  end(): void
  /** Number of deltas currently buffered by the coalescer. */
  readonly buffered: number
  readonly ended: boolean
  /** The control-plane client, when a `controlPort` was supplied. */
  readonly mesh: MeshWorkerClient | undefined
  /** End the data stream and dispose the mesh link. Idempotent. */
  dispose(): void
}

/** The lifecycle log fields shared by the connect/close lines for this peer. */
const egressLifecycleFields = (workerID: number | undefined, level: "info" | "warn"): Record<string, unknown> => ({
  peer: "egress",
  level,
  ...(workerID === undefined ? {} : { workerID }),
})

/**
 * Adapt the worker's egress ports into a client. The data plane is the sender
 * over `port`; the optional control plane is the mesh client over `controlPort`.
 *
 * Emits the E2 peer lifecycle lines: an INFO `egress connected` once the sender
 * (and mesh, when supplied) is adapted, a WARN `egress connect failed` (with
 * `reason`) if construction throws, and an INFO `egress closed` at dispose.
 */
export function createWorkerEgressClient(
  port: MessagePort,
  options: WorkerEgressClientOptions = {},
): WorkerEgressClient {
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID
  try {
    const client = buildEgressClient(port, options)
    log(workerLogMessage("egress connected"), egressLifecycleFields(workerID, "info"))
    return client
  } catch (error) {
    log(workerLogMessage("egress connect failed"), {
      ...egressLifecycleFields(workerID, "warn"),
      reason: messageOf(error),
    })
    throw error
  }
}

/** The connect seam: adapt the data and control ports; a start failure throws. */
function buildEgressClient(port: MessagePort, options: WorkerEgressClientOptions): WorkerEgressClient {
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID
  const sender: EgressSender = createEgressSender({
    port,
    ...(options.generation === undefined ? {} : { generation: options.generation }),
    ...(options.windowMs === undefined ? {} : { windowMs: options.windowMs }),
    ...(options.maxBatch === undefined ? {} : { maxBatch: options.maxBatch }),
    ...(options.scheduler === undefined ? {} : { scheduler: options.scheduler }),
  })
  const mesh = options.controlPort === undefined ? undefined : createMeshWorkerClient(options.controlPort)
  let disposed = false

  return {
    emit: (event) => sender.pushDelta(event),
    emitDurable: (event, meta) => sender.pushDurable(event, meta),
    end: () => sender.end(),
    get buffered() {
      return sender.buffered
    },
    get ended() {
      return sender.ended
    },
    mesh,
    dispose: () => {
      if (disposed) return
      disposed = true
      sender.end()
      mesh?.dispose()
      log(workerLogMessage("egress closed"), egressLifecycleFields(workerID, "info"))
    },
  }
}

export * as WorkerEgressClientModule from "./egress-client"
