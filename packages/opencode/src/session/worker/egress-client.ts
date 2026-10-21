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
import { createEgressSender } from "@/session/egress/sender"

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

export function createWorkerEgressClient(
  port: MessagePort,
  options: WorkerEgressClientOptions = {},
): WorkerEgressClient {
  const sender = createEgressSender({
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
    },
  }
}

export * as WorkerEgressClientModule from "./egress-client"
