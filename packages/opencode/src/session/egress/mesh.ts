// EventV2 cross-isolate mesh.

import { createControlLink, isNonNegativeInteger, type ControlInbound, type ControlOutbound } from "./control"
import type { AggregatePosition, EventV2Event } from "./frame"

/** Per-worker subscription backlog bound. */
export const MESH_SUBSCRIPTION_BOUND = 256

export interface DurableCommit {
  readonly meta: AggregatePosition
  readonly event: EventV2Event
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
  /** Ask one pattern to resync; `position` is the commit that overflowed. */
  sendResync(pattern: string, position?: AggregatePosition): void
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

/** Match an event type against a subscription pattern. */
export function matchesMeshPattern(pattern: string, type: string): boolean {
  if (pattern === "*") return true
  if (pattern.endsWith("*")) return type.startsWith(pattern.slice(0, -1))
  return pattern === type
}

/** Run a best-effort push; a failing sink must never abort the whole fan-out. */
function bestEffort(push: () => void): boolean {
  try {
    push()
    return true
  } catch {
    return false
  }
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
  const matchingPatterns = (workerId: string, type: string): string[] => {
    const patterns = subscriptions.get(workerId)
    if (!patterns) return []
    const matched: string[] = []
    for (const pattern of patterns) {
      if (matchesMeshPattern(pattern, type)) matched.push(pattern)
    }
    return matched
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
        const patterns = matchingPatterns(workerId, commit.event.type)
        if (patterns.length === 0) continue
        const sink = sinks.get(workerId)
        const pending = outstanding.get(workerId) ?? 0
        if (pending >= bound) {
          dropped.push(workerId)
          // EVERY matching subscription has a gap in its own stream, so every one
          // is resynced with the commit that was dropped for this worker.
          for (const pattern of patterns) {
            flagResync(workerId, pattern)
            if (sink) bestEffort(() => sink.sendResync(pattern, commit.meta))
          }
          continue
        }
        // A delivery counts only once its push succeeded: a closed or failing
        // port must not inflate `outstanding` or claim a worker it never reached.
        if (sink && !bestEffort(() => sink.sendDurable(commit))) continue
        outstanding.set(workerId, pending + 1)
        delivered.push(workerId)
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

// --------------------------------------------------------------------------- Transport wiring
// ---------------------------------------------------------------------------

export interface MeshWorkerAttachment {
  dispose(): void
}

/**
 * Parse a `durable.ack` count: absent means a single delivery; anything other than a non-negative integer
 * fails loud, so a corrupt ack can never inflate or corrupt the outstanding-delivery counter.
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
    sendResync: (pattern, position) =>
      link.notify({
        type: "resync",
        pattern,
        ...(position === undefined ? {} : { meta: position }),
      } satisfies ControlOutbound),
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
  onResync(handler: (pattern: string, position?: AggregatePosition) => void): void
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
  let resyncHandler: ((pattern: string, position?: AggregatePosition) => void) | undefined

  link.onMessage((frame) => {
    if (frame.type === "durable") {
      durableHandler?.({ meta: frame.meta, event: frame.event })
      return
    }
    if (frame.type === "resync" && typeof frame.pattern === "string") {
      resyncHandler?.(frame.pattern, frame.meta)
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
