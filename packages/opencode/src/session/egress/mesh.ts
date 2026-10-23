// Phase 8.9 + 8.10 + 8.11 — EventV2 cross-isolate mesh (plan tasks 8.9–8.11).
//
// `events.listen` is isolate-local, so a worker's `durable()` subscribers can
// only be woken by the hub. The hub keeps a registry keyed by `(worker, pattern)`
// — a worker sends a `listen` request over its control port and receives
// `durable` frames for matching commits (task 8.10). Each worker's outbound
// backlog is bounded by an OUTSTANDING-DELIVERY COUNTER (~256): every delivery
// the worker has not yet acknowledged occupies one slot. An over-full worker
// drops the commit and is sent a `resync` instruction instead of blocking the
// mesh (task 8.11); acknowledging a delivery frees its slot, so a draining
// worker keeps receiving.

import { createControlLink, isNonNegativeInteger, type ControlInbound, type ControlOutbound } from "./control"
import type { EgressEvent } from "./frame"
import type { WriterOrdinal } from "./ordinal"

/** Per-worker subscription backlog bound (plan decision line 87). */
export const MESH_SUBSCRIPTION_BOUND = 256

export interface DurableCommit {
  readonly meta: WriterOrdinal
  readonly event: EgressEvent
}

export interface MeshSubscription {
  readonly workerId: string
  readonly pattern: string
}

export interface MeshPublishResult {
  /** Worker ids that accepted the commit. */
  readonly delivered: readonly string[]
  /** Worker ids that overflowed and were told to resync. */
  readonly dropped: readonly string[]
}

/** Hub → worker transport for the mesh; the hub passes these to `attach`. */
export interface MeshSink {
  sendDurable(commit: DurableCommit): void
  sendResync(pattern: string): void
}

export interface EgressMesh {
  readonly bound: number
  listen(workerId: string, pattern: string): void
  unlisten(workerId: string, pattern: string): void
  publish(commit: DurableCommit): MeshPublishResult
  /** Register a worker's transport sink; returns a detach function. */
  attach(workerId: string, sink: MeshSink): () => void
  /** Un-acknowledged deliveries outstanding for `workerId` — the backpressure depth. */
  outstanding(workerId: string): number
  /** Acknowledge `count` delivered commits, freeing that many backlog slots. */
  ack(workerId: string, count?: number): void
  /** Worker ids with at least one pattern awaiting resync. */
  readonly resyncPending: readonly string[]
  /** Acknowledge resync for one `pattern`, or all of `workerId`'s when omitted. */
  acknowledgeResync(workerId: string, pattern?: string): void
  subscriptions(): readonly MeshSubscription[]
}

/**
 * Match an event type against a subscription pattern.
 *
 * `"*"` matches everything, a trailing `"*"` is a prefix match, and everything
 * else is an exact match. Kept deliberately small — the hub registry only needs
 * to route per-worker wakeups.
 */
export function matchesMeshPattern(pattern: string, type: string): boolean {
  if (pattern === "*") return true
  if (pattern.endsWith("*")) return type.startsWith(pattern.slice(0, -1))
  return pattern === type
}

export interface EgressMeshOptions {
  readonly bound?: number
}

export function createEgressMesh(options: EgressMeshOptions = {}): EgressMesh {
  const bound = options.bound ?? MESH_SUBSCRIPTION_BOUND
  if (!Number.isInteger(bound) || bound < 1) {
    throw new Error(`invalid mesh subscription bound: ${bound} (expected a positive integer)`)
  }

  const subscriptions = new Map<string, Set<string>>()
  const outstanding = new Map<string, number>()
  const sinks = new Map<string, MeshSink>()
  const resync = new Map<string, Set<string>>()

  /** Pure read: does NOT create a subscription entry for `workerId`. */
  const firstMatchingPattern = (workerId: string, type: string): string | undefined => {
    const patterns = subscriptions.get(workerId)
    if (!patterns) return undefined
    for (const pattern of patterns) {
      if (matchesMeshPattern(pattern, type)) return pattern
    }
    return undefined
  }

  const flagResync = (workerId: string, pattern: string): void => {
    const patterns = resync.get(workerId)
    if (patterns) {
      patterns.add(pattern)
      return
    }
    resync.set(workerId, new Set([pattern]))
  }

  const forget = (workerId: string): void => {
    subscriptions.delete(workerId)
    outstanding.delete(workerId)
    resync.delete(workerId)
  }

  return {
    bound,
    listen(workerId, pattern) {
      const patterns = subscriptions.get(workerId)
      if (patterns) {
        patterns.add(pattern)
        return
      }
      subscriptions.set(workerId, new Set([pattern]))
    },
    unlisten(workerId, pattern) {
      const patterns = subscriptions.get(workerId)
      if (!patterns) return
      patterns.delete(pattern)
      if (patterns.size === 0) subscriptions.delete(workerId)
    },
    publish(commit) {
      const delivered: string[] = []
      const dropped: string[] = []
      for (const workerId of subscriptions.keys()) {
        const pattern = firstMatchingPattern(workerId, commit.event.type)
        if (pattern === undefined) continue
        const pending = outstanding.get(workerId) ?? 0
        if (pending >= bound) {
          dropped.push(workerId)
          flagResync(workerId, pattern)
          sinks.get(workerId)?.sendResync(pattern)
          continue
        }
        outstanding.set(workerId, pending + 1)
        delivered.push(workerId)
        sinks.get(workerId)?.sendDurable(commit)
      }
      return { delivered, dropped }
    },
    attach(workerId, sink) {
      sinks.set(workerId, sink)
      return () => {
        if (sinks.get(workerId) !== sink) return
        sinks.delete(workerId)
        forget(workerId)
      }
    },
    outstanding: (workerId) => outstanding.get(workerId) ?? 0,
    ack(workerId, count = 1) {
      const pending = outstanding.get(workerId)
      if (pending === undefined) return
      const remaining = pending - count
      if (remaining > 0) {
        outstanding.set(workerId, remaining)
        return
      }
      outstanding.delete(workerId)
    },
    get resyncPending() {
      return [...resync.keys()]
    },
    acknowledgeResync(workerId, pattern) {
      const patterns = resync.get(workerId)
      if (!patterns) return
      if (pattern === undefined) {
        resync.delete(workerId)
        return
      }
      patterns.delete(pattern)
      if (patterns.size === 0) resync.delete(workerId)
    },
    subscriptions() {
      const all: MeshSubscription[] = []
      for (const [workerId, patterns] of subscriptions) {
        for (const pattern of patterns) all.push({ workerId, pattern })
      }
      return all
    },
  }
}

// ---------------------------------------------------------------------------
// Transport wiring
// ---------------------------------------------------------------------------

export interface MeshWorkerAttachment {
  dispose(): void
}

/**
 * Parse a `durable.ack` count: absent means a single delivery; anything other
 * than a non-negative integer fails loud, so a corrupt ack can never inflate or
 * corrupt the outstanding-delivery counter.
 */
function requireAckCount(value: unknown): number {
  if (value === undefined) return 1
  if (!isNonNegativeInteger(value)) {
    throw new Error(`invalid durable.ack count: ${JSON.stringify(value)} (expected a non-negative integer)`)
  }
  return value
}

/** Hub end of a worker's mesh control link: routes `listen`/`unlisten`/acks, pushes wakes. */
export function attachMeshWorker(mesh: EgressMesh, workerId: string, port: MessagePort): MeshWorkerAttachment {
  const link = createControlLink(port)
  const detach = mesh.attach(workerId, {
    sendDurable: (commit) =>
      link.notify({ type: "durable", meta: commit.meta, event: commit.event } satisfies ControlOutbound),
    sendResync: (pattern) => link.notify({ type: "resync", pattern } satisfies ControlOutbound),
  })

  const replyOk = (frame: ControlInbound): void => {
    if ("id" in frame) link.notify({ type: "listen.ok", id: frame.id })
  }

  link.onMessage((frame) => {
    if (frame.type === "listen" && typeof frame.pattern === "string") {
      mesh.listen(workerId, frame.pattern)
      replyOk(frame)
      return
    }
    if (frame.type === "unlisten" && typeof frame.pattern === "string") {
      mesh.unlisten(workerId, frame.pattern)
      replyOk(frame)
      return
    }
    if (frame.type === "durable.ack") {
      mesh.ack(workerId, requireAckCount(frame.count))
      return
    }
    if (frame.type === "resync.ack") {
      mesh.acknowledgeResync(workerId, typeof frame.pattern === "string" ? frame.pattern : undefined)
    }
  })

  return {
    dispose: () => {
      detach()
      link.dispose()
    },
  }
}

export interface MeshWorkerClient {
  listen(pattern: string): Promise<void>
  unlisten(pattern: string): Promise<void>
  /** Wake registered `durable()` subscribers for a matching committed event. */
  onDurable(handler: (commit: DurableCommit) => void): void
  onResync(handler: (pattern: string) => void): void
  /** Tell the hub that `count` delivered commits were processed (frees backlog slots). */
  acknowledgeDelivery(count?: number): void
  /** Tell the hub that the named pattern's resync completed. */
  acknowledgeResync(pattern: string): void
  dispose(): void
}

/** Worker end of the mesh control link. */
export function createMeshWorkerClient(port: MessagePort): MeshWorkerClient {
  const link = createControlLink(port)
  let durableHandler: ((commit: DurableCommit) => void) | undefined
  let resyncHandler: ((pattern: string) => void) | undefined

  link.onMessage((frame) => {
    if (frame.type === "durable") {
      durableHandler?.({ meta: frame.meta, event: frame.event })
      return
    }
    if (frame.type === "resync" && typeof frame.pattern === "string") {
      resyncHandler?.(frame.pattern)
    }
  })

  return {
    listen: async (pattern) => {
      await link.request({ type: "listen", pattern })
    },
    unlisten: async (pattern) => {
      await link.request({ type: "unlisten", pattern })
    },
    onDurable: (handler) => {
      durableHandler = handler
    },
    onResync: (handler) => {
      resyncHandler = handler
    },
    acknowledgeDelivery: (count = 1) =>
      link.notify({ type: "durable.ack", count: requireAckCount(count) } satisfies ControlInbound),
    acknowledgeResync: (pattern) => link.notify({ type: "resync.ack", pattern } satisfies ControlInbound),
    dispose: () => link.dispose(),
  }
}

export * as EgressMeshModule from "./mesh"
