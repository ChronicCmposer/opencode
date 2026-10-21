// Phase 8.9 + 8.10 + 8.11 — EventV2 cross-isolate mesh (plan tasks 8.9–8.11).
//
// `events.listen` is isolate-local, so a worker's `durable()` subscribers can
// only be woken by the hub. The hub keeps a registry keyed by `(worker, pattern)`
// — a worker sends a `listen` request over its control port and receives
// `durable` frames for matching commits (task 8.10). Each worker's outbound
// backlog is bounded (~256): an over-full worker drops the commit and is sent a
// `resync` instruction instead of blocking the mesh (task 8.11).

import { createControlLink, type ControlEnvelope } from "./control"
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
  /** Remove and return the commits queued for `workerId`, oldest first. */
  drain(workerId: string): readonly DurableCommit[]
  depth(workerId: string): number
  /** Acknowledge `count` delivered commits, lowering the backlog. */
  ack(workerId: string, count?: number): void
  readonly resyncPending: readonly string[]
  acknowledgeResync(workerId: string): void
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
  const queues = new Map<string, DurableCommit[]>()
  const sinks = new Map<string, MeshSink>()
  const resync = new Set<string>()

  const patternsFor = (workerId: string): Set<string> => {
    const existing = subscriptions.get(workerId)
    if (existing) return existing
    const created = new Set<string>()
    subscriptions.set(workerId, created)
    return created
  }

  const matchingPattern = (workerId: string, type: string): string | undefined => {
    for (const pattern of patternsFor(workerId)) {
      if (matchesMeshPattern(pattern, type)) return pattern
    }
    return undefined
  }

  return {
    bound,
    listen(workerId, pattern) {
      patternsFor(workerId).add(pattern)
    },
    unlisten(workerId, pattern) {
      subscriptions.get(workerId)?.delete(pattern)
    },
    publish(commit) {
      const delivered: string[] = []
      const dropped: string[] = []
      for (const workerId of subscriptions.keys()) {
        const pattern = matchingPattern(workerId, commit.event.type)
        if (pattern === undefined) continue
        const queue = queues.get(workerId) ?? []
        if (queue.length >= bound) {
          dropped.push(workerId)
          resync.add(workerId)
          sinks.get(workerId)?.sendResync(pattern)
          continue
        }
        queue.push(commit)
        queues.set(workerId, queue)
        delivered.push(workerId)
        sinks.get(workerId)?.sendDurable(commit)
      }
      return { delivered, dropped }
    },
    attach(workerId, sink) {
      sinks.set(workerId, sink)
      return () => {
        if (sinks.get(workerId) === sink) sinks.delete(workerId)
      }
    },
    drain(workerId) {
      const queue = queues.get(workerId)
      if (!queue) return []
      queues.set(workerId, [])
      return queue
    },
    depth: (workerId) => queues.get(workerId)?.length ?? 0,
    ack(workerId, count = 1) {
      const queue = queues.get(workerId)
      if (!queue) return
      queue.splice(0, count)
    },
    get resyncPending() {
      return [...resync]
    },
    acknowledgeResync(workerId) {
      resync.delete(workerId)
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

/** Hub end of a worker's mesh control link: routes `listen`/`unlisten`, pushes wakes. */
export function attachMeshWorker(mesh: EgressMesh, workerId: string, port: MessagePort): MeshWorkerAttachment {
  const link = createControlLink(port)
  const detach = mesh.attach(workerId, {
    sendDurable: (commit) => link.notify({ type: "durable", meta: commit.meta, event: commit.event }),
    sendResync: (pattern) => link.notify({ type: "resync", pattern }),
  })

  const replyOk = (frame: ControlEnvelope): void => {
    if (typeof frame.id === "number") link.notify({ type: "listen.ok", id: frame.id })
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
    if (frame.type === "resync.ack") {
      mesh.acknowledgeResync(workerId)
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
      durableHandler?.({ meta: frame.meta as WriterOrdinal, event: frame.event as EgressEvent })
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
    acknowledgeResync: (pattern) => link.notify({ type: "resync.ack", pattern }),
    dispose: () => link.dispose(),
  }
}

export * as EgressMeshModule from "./mesh"
