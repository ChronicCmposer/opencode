// W2 — worker-side egress client over the events envelope (spec Q6/Q7/Q21).
//
// The reduced session-worker graph talks to the hub event channel through a
// dedicated, transferred `MessagePort`. This module is the
// adapter that turns the worker's EventV2 events into framed bytes on the wire:
//
//   * the DATA plane is `createEgressSender` (`egress/sender.ts`) — coalescing
//     non-durable deltas and posting shared-envelope JSON frames;
//   * the CONTROL plane is `createMeshWorkerClient` (`egress/mesh.ts`) over a
//     SEPARATE port, so durable wakeups and resync never queue behind deltas.
//
// Events cross VERBATIM as `{ id, type, properties }` — there is no `data`
// translation. `emit` maps to a coalesced non-durable delta; `emitDurable`
// requires the commit's aggregate `(aggregateID, seq)` and bypasses coalescing.

import type { EventV2Event } from "@/session/worker/frame"

import type { CoalescerScheduler } from "@/session/egress/coalesce"
import { decodeFrame, type AggregatePosition, type DurableMeta } from "@/session/egress/frame"
import {
  createMeshWorkerClient,
  matchesMeshPattern,
  type DurableCommit,
  type MeshWorkerClient,
} from "@/session/egress/mesh"
import { createEgressSender, type EgressSender } from "@/session/egress/sender"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"
import { connectPeer, messageOf, peerLifecycle } from "./port"

export interface WorkerEgressClientOptions {
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

/** One worker-local durable subscription, driven through the mesh control plane. */
export interface WorkerDurableSubscription {
  /** Event-type pattern the hub routes matching durable commits for. */
  readonly pattern: string
  /** Called for each matching durable commit; the delivery is acked once it returns. */
  readonly onDurable: (commit: DurableCommit) => void
  /**
   * Called when this pattern must resync: either it overflowed at the hub or the
   * hub observed a gap in the worker's durable stream. `position` is the exact
   * aggregate/sequence to heal from the database when known.
   */
  readonly onResync?: (pattern: string, position?: AggregatePosition) => void
}

export interface WorkerEgressClient {
  /**
   * Emit a NON-DURABLE delta (the shape `SessionPipeline.run` uses for streaming
   * parts). Deltas are coalesced and may be dropped under backpressure.
   */
  emit(event: EventV2Event): void
  /**
   * Emit a DURABLE boundary event — one that must be observable exactly once and
   * never dropped. Its aggregate `(aggregateID, seq)` is mandatory; there is no
   * sender-global ordinal fallback. A committed `session.text` boundary is
   * durable, so callers must prefer this over `emit`.
   */
  emitDurable(event: EventV2Event, meta: DurableMeta): void
  /** Flush buffered deltas, then emit the `end` frame (idempotent). */
  end(): void
  /** Number of deltas currently buffered by the coalescer. */
  readonly buffered: number
  readonly ended: boolean
  /**
   * The control-plane client, when a `controlPort` was supplied. Exposed for
   * manual driving; {@link WorkerEgressSubscriptionClient.subscribeDurable} is the
   * managed path that wires `listen` and routes `onDurable`/`onResync`.
   */
  readonly mesh: MeshWorkerClient | undefined
  /** End the data stream and dispose the mesh link. Idempotent. */
  dispose(): void
}

/**
 * A `WorkerEgressClient` produced by the factory, so a control plane was wired
 * and durable subscriptions can be driven. A degraded or structural client (one
 * with no control port, e.g. the hub-less fallback) still satisfies the base
 * `WorkerEgressClient`, which deliberately has no subscription surface.
 */
export interface WorkerEgressSubscriptionClient extends WorkerEgressClient {
  /**
   * Register a durable subscription with the hub mesh: calls `mesh.listen(pattern)`
   * and routes matching `onDurable` commits (and `onResync` overflows) to the
   * subscriber. Each delivery/resync is acknowledged once handled, freeing the
   * hub's per-worker backlog slot. Returns an unsubscribe function. Fails loud
   * when no `controlPort` was supplied, since a durable subscription can never be
   * woken without the mesh.
   */
  subscribeDurable(subscription: WorkerDurableSubscription): () => void
}

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
): WorkerEgressSubscriptionClient {
  return connectPeer(options.log ?? noopWorkerLogger, "egress", options.workerID, () =>
    buildEgressClient(port, options),
  )
}

/** The connect seam: adapt the data and control ports; a start failure throws. */
function buildEgressClient(port: MessagePort, options: WorkerEgressClientOptions): WorkerEgressSubscriptionClient {
  const log = options.log ?? noopWorkerLogger
  const workerID = options.workerID
  const sender: EgressSender = createEgressSender({
    port,
    ...(options.windowMs === undefined ? {} : { windowMs: options.windowMs }),
    ...(options.maxBatch === undefined ? {} : { maxBatch: options.maxBatch }),
    ...(options.scheduler === undefined ? {} : { scheduler: options.scheduler }),
  })
  const mesh = options.controlPort === undefined ? undefined : createMeshWorkerClient(options.controlPort)
  const subscriptions = new Map<string, WorkerDurableSubscription>()
  let meshWired = false
  let disposed = false

  /**
   * Does a local subscription need the resync for `pattern`? An exact match, a
   * catch-all local subscription, or a catch-all resync all qualify; a gap
   * resync with an aggregate position carries no subscription pattern at all, so
   * every live subscription is told (its own predicate decides relevance).
   */
  const resyncTargets = (pattern: string | undefined): WorkerDurableSubscription[] => {
    // A gap resync names no subscription pattern: every live subscription heals.
    if (pattern === undefined) return [...subscriptions.values()]
    // Otherwise the hub already fanned the resync out per registered pattern.
    // Prefer the exact local entry; fall back to a local catch-all only when this
    // worker did not register the pattern itself.
    const exact = subscriptions.get(pattern)
    if (exact !== undefined) return [exact]
    return [...subscriptions.values()].filter((subscription) => subscription.pattern === "*")
  }

  /** Surface one resync to every matching subscription, without letting one throw starve the rest. */
  const broadcastResync = (pattern: string | undefined, position: AggregatePosition | undefined): void => {
    for (const subscription of resyncTargets(pattern)) subscription.onResync?.(subscription.pattern, position)
  }

  // Wire the mesh link ONCE: the client multiplexes every local subscription
  // through the single `onDurable`/`onResync` handler the mesh link exposes.
  const wireMesh = (control: MeshWorkerClient): void => {
    if (meshWired) return
    meshWired = true
    control.onDurable((commit) => {
      // Ack in `finally`: a throwing subscriber is a caller bug (fail loud) but
      // must not leak the hub's per-worker backlog slot.
      try {
        for (const subscription of subscriptions.values()) {
          if (!matchesMeshPattern(subscription.pattern, commit.event.type)) continue
          subscription.onDurable(commit)
        }
      } finally {
        // One hub delivery occupies one slot regardless of how many local
        // subscribers matched, so ack exactly once per commit.
        control.acknowledgeDelivery()
      }
    })
    control.onResync((pattern, position) => {
      try {
        broadcastResync(pattern, position)
      } finally {
        control.acknowledgeResync(pattern)
      }
    })
  }

  // The events data port is bidirectional: the hub sends a `resync` frame there
  // when its receiver observed a durable sequence gap. Route it through the same
  // subscription fan-out the mesh uses, so the worker heals from its DB snapshot.
  const onDataMessage = (message: MessageEvent): void => {
    if (typeof message.data !== "string") return
    try {
      const frame = decodeFrame(message.data)
      if (frame.kind !== "resync") return
      broadcastResync(
        frame.payload?.pattern,
        frame.payload?.aggregateID === undefined || frame.payload.seq === undefined
          ? undefined
          : { aggregateID: frame.payload.aggregateID, seq: frame.payload.seq },
      )
    } catch (error) {
      log(workerLogMessage("egress resync frame rejected"), {
        ...peerLifecycle("egress", workerID)("warn"),
        reason: messageOf(error),
      })
    }
  }
  port.onmessage = onDataMessage
  port.start()

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
    subscribeDurable(subscription) {
      const control = mesh
      if (control === undefined) {
        throw new Error(
          `worker egress client has no control port; cannot subscribe to durable pattern "${subscription.pattern}"`,
        )
      }
      wireMesh(control)
      subscriptions.set(subscription.pattern, subscription)
      void control.listen(subscription.pattern).catch((error) => {
        // The hub never registered the pattern, so drop the local entry: a stale
        // subscription would route nothing yet outlive its unsubscribe handle.
        if (subscriptions.get(subscription.pattern) === subscription) subscriptions.delete(subscription.pattern)
        log(workerLogMessage("egress durable subscribe failed"), {
          ...peerLifecycle("egress", workerID)("warn"),
          pattern: subscription.pattern,
          reason: messageOf(error),
        })
      })
      return () => {
        if (subscriptions.get(subscription.pattern) !== subscription) return
        subscriptions.delete(subscription.pattern)
        void control.unlisten(subscription.pattern).catch(() => undefined)
      }
    },
    dispose: () => {
      if (disposed) return
      disposed = true
      subscriptions.clear()
      sender.end()
      mesh?.dispose()
      log(workerLogMessage("egress closed"), peerLifecycle("egress", workerID)("info"))
    },
  }
}

export * as WorkerEgressClientModule from "./egress-client"
