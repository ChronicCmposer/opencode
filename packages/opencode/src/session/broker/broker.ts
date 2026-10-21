// Phase 10 — hub-side MCP/LSP broker client (plan tasks 10.2–10.9).
//
// Spawns the dedicated broker Worker, completes the port-pairing handshake,
// and routes request-id-tagged frames over a transferred `MessagePort`, so no
// MCP/LSP I/O ever runs on the hub loop. It also supervises the broker:
//
//   - a worker exit/crash rejects every in-flight call (`crashed`),
//   - the hub mirrors child PGIDs and reaps orphans on crash (POSIX group kill),
//   - the broker respawns with a bumped generation and a rebroadcast port,
//   - a supervision signal path notifies observers (`crashed` / `respawned`).
//
// The contract intentionally depends on nothing from the pool/writer/egress
// modules: it is a leaf service the hub can own directly.

import { DEFAULT_MCP_TIMEOUT_MS, parseAcquireResult, parseOAuthResult, parsePingResult } from "./protocol"
import type {
  AcquireResult,
  BrokerErrorCode,
  BrokerFrame,
  BrokerRequest,
  OAuthResult,
  PingResult,
  ServerSpec,
} from "./protocol"
import { mergeEnv } from "./env"

export interface WorkerLike {
  readonly generation: number
  readonly alive: boolean
}

export interface PortLike {
  close(): void
}

export interface BrokerChild {
  readonly directory: string
  readonly server: string
  readonly pid: number
  readonly pgid: number
}

export type SupervisionEvent =
  | { readonly type: "crashed"; readonly generation: number; readonly orphans: number }
  | { readonly type: "respawned"; readonly generation: number }

/** Kills a leaked child process group; injectable so tests can observe reaping. */
export interface ProcessReaper {
  reap(pgid: number): void
}

/** Raised for a broker-level failure; `code` mirrors the wire error taxonomy. */
export class BrokerRequestError extends Error {
  readonly code: BrokerErrorCode
  constructor(code: BrokerErrorCode, message: string) {
    super(message)
    this.name = "BrokerRequestError"
    this.code = code
  }
}

export interface BrokerOptions {
  /** Broker worker entry; defaults to the production `broker-worker.ts`. */
  readonly entry?: string | URL
  /** Environment overlay applied to the broker worker. */
  readonly env?: Record<string, string | undefined>
  /** Hub-side request timeout in ms; defaults to the broker `mcp_timeout`. */
  readonly timeoutMs?: number
  /** Override orphan reaping (defaults to a POSIX process-group kill). */
  readonly reaper?: ProcessReaper
}

export interface AcquireInput {
  readonly directory: string
  readonly servers: ReadonlyArray<ServerSpec>
}

export interface CallInput {
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
  readonly timeoutMs?: number
  /** Explicit request id, so a caller can cancel it by id. */
  readonly requestId?: string
}

export interface NotificationLike {
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
}

export interface BrokerClient extends AsyncDisposable {
  readonly ready: Promise<void>
  readonly generation: number
  readonly threadId: number
  readonly alive: boolean
  readonly transport: "message-port"
  readonly handshake: ReadonlyArray<string>
  readonly workerHandle: WorkerLike
  readonly portHandle: PortLike
  /** Mirrored live children, keyed by (directory, server). */
  readonly children: ReadonlyArray<BrokerChild>
  acquire(input: AcquireInput): Promise<AcquireResult>
  call(input: CallInput): Promise<unknown>
  cancel(targetId: string): Promise<void>
  oauth(directory: string): Promise<OAuthResult>
  release(directory: string): Promise<void>
  ping(): Promise<PingResult>
  teardown(): Promise<void>
  onNotification(handler: (notification: NotificationLike) => void): () => void
  onSupervision(handler: (event: SupervisionEvent) => void): () => void
}

const DEFAULT_ENTRY = new URL("./broker-worker.ts", import.meta.url)

const defaultReaper: ProcessReaper = {
  reap(pgid: number): void {
    try {
      process.kill(-pgid, "SIGTERM")
    } catch {
      /* best-effort: the group is already gone (ESRCH) or not ours to kill */
    }
  },
}

/** Minimal structural view of a Bun Worker, avoiding DOM lib coupling. */
interface BrokerWorkerHandle {
  onmessage: ((event: { data: unknown }) => void) | null
  addEventListener(type: string, listener: () => void): void
  removeEventListener(type: string, listener: () => void): void
  postMessage(message: unknown, transfer?: ReadonlyArray<Transferable>): void
  terminate(): Promise<number> | number
}

interface PendingRequest {
  readonly resolve: (value: unknown) => void
  readonly reject: (error: Error) => void
  readonly timer: ReturnType<typeof setTimeout>
}

function keyOf(directory: string, server: string): string {
  return `${directory}\u0000${server}`
}

export function createBroker(options: BrokerOptions = {}): BrokerClient {
  const entry = options.entry ?? DEFAULT_ENTRY
  const env = mergeEnv(options.env)
  const reaper = options.reaper ?? defaultReaper
  const requestTimeoutMs = options.timeoutMs ?? DEFAULT_MCP_TIMEOUT_MS

  let generation = 1
  let threadId = 0
  let alive = false
  let disposed = false
  let currentWorkerId = 0
  let nextRequestId = 0

  const handshake: string[] = []
  const mirror = new Map<string, BrokerChild>()
  const pending = new Map<string, PendingRequest>()
  const notificationHandlers = new Set<(notification: NotificationLike) => void>()
  const supervisionHandlers = new Set<(event: SupervisionEvent) => void>()

  let worker: BrokerWorkerHandle | undefined
  let port: MessagePort | undefined
  let readySettled = false

  let resolveTransport: (() => void) | undefined
  let rejectTransport: ((error: Error) => void) | undefined

  /**
   * A fresh per-generation transport deferred. A crash before any request is
   * waiting must not surface as an unhandled rejection, so the eager catch is
   * attached here; `request` installs its own handlers on top.
   */
  function newTransportReady(): Promise<void> {
    const promise = new Promise<void>((resolve, reject) => {
      resolveTransport = resolve
      rejectTransport = reject
    })
    promise.catch(() => {})
    return promise
  }

  let transportReady = newTransportReady()

  let settleReady!: () => void
  const ready = new Promise<void>((resolve) => {
    settleReady = resolve
  })

  const workerHandle: WorkerLike = {
    get generation() {
      return generation
    },
    get alive() {
      return alive
    },
  }
  const portHandle: PortLike = {
    close: () => port?.close(),
  }

  function emitSupervision(event: SupervisionEvent): void {
    for (const handler of [...supervisionHandlers]) handler(event)
  }

  function handleFrame(frame: BrokerFrame): void {
    switch (frame.kind) {
      case "response": {
        const request = pending.get(frame.requestId)
        if (!request) return
        pending.delete(frame.requestId)
        clearTimeout(request.timer)
        if (frame.ok) request.resolve(frame.value)
        else request.reject(new BrokerRequestError(frame.error.code, frame.error.message))
        return
      }
      case "notification": {
        for (const handler of [...notificationHandlers]) handler(frame)
        return
      }
      case "child": {
        mirror.set(keyOf(frame.directory, frame.server), {
          directory: frame.directory,
          server: frame.server,
          pid: frame.pid,
          pgid: frame.pgid,
        })
        return
      }
      case "child-exit": {
        mirror.delete(keyOf(frame.directory, frame.server))
        return
      }
    }
  }

  function nextId(): string {
    return `r${++nextRequestId}`
  }

  function request(frame: BrokerRequest): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      let settled = false
      // Declared before `finish` (no TDZ) and definitely assigned before any
      // settle path can run, since all paths are asynchronous.
      let timer!: ReturnType<typeof setTimeout>
      const finish = (action: () => void) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        action()
      }
      // Arm the timeout around the whole request, including the wait for the
      // transport: a worker that crashes (or never becomes ready) before the
      // port is live must not hang the caller forever.
      timer = setTimeout(() => {
        pending.delete(frame.requestId)
        finish(() =>
          reject(new BrokerRequestError("timeout", `request ${frame.requestId} timed out after ${requestTimeoutMs}ms`)),
        )
      }, requestTimeoutMs)
      const send = () => {
        if (!alive || !port) {
          finish(() => reject(new BrokerRequestError("crashed", "broker is not available")))
          return
        }
        pending.set(frame.requestId, { resolve, reject, timer })
        port.postMessage(frame)
      }
      transportReady.then(send, (error) => finish(() => reject(error)))
    })
  }

  function rejectPending(error: Error): void {
    for (const request of pending.values()) {
      clearTimeout(request.timer)
      request.reject(error)
    }
    pending.clear()
  }

  function handleCrash(): void {
    const orphans = [...mirror.values()]
    mirror.clear()
    rejectPending(new BrokerRequestError("crashed", `broker worker exited (generation ${generation})`))
    alive = false
    threadId = 0
    port?.close()
    port = undefined
    // Settle the outgoing transport before reassigning it, so any request caught
    // waiting for the port fails fast instead of dangling on the old promise.
    rejectTransport?.(new BrokerRequestError("crashed", `broker worker exited before ready (generation ${generation})`))
    emitSupervision({ type: "crashed", generation, orphans: orphans.length })
    for (const child of orphans) reaper.reap(child.pgid)
    generation += 1
    transportReady = newTransportReady()
    startWorker()
  }

  function startWorker(): void {
    const myId = ++currentWorkerId
    // Each generation gets its own handshake trace; reset before it re-runs.
    handshake.length = 0
    const handle = new Worker(entry, { env }) as unknown as BrokerWorkerHandle
    worker = handle

    handle.onmessage = (event) => {
      if (myId !== currentWorkerId) return
      const data = event.data as { type?: string; threadId?: number; port?: MessagePort }
      if (data?.type === "ready") {
        threadId = data.threadId ?? 0
        handshake.push("ready")
        const channel = new MessageChannel()
        port = channel.port1
        port.onmessage = (message) => handleFrame(message.data as BrokerFrame)
        port.start?.()
        handle.postMessage({ type: "attach", port: channel.port2 }, [channel.port2 as unknown as Transferable])
        handshake.push("attach")
        return
      }
      if (data?.type === "attached") {
        handshake.push("attached")
        alive = true
        resolveTransport?.()
        resolveTransport = undefined
        rejectTransport = undefined
        if (generation > 1) emitSupervision({ type: "respawned", generation })
        if (!readySettled) {
          readySettled = true
          settleReady()
        }
      }
    }

    const onGone = () => {
      if (disposed || myId !== currentWorkerId) return
      handleCrash()
    }
    handle.addEventListener("close", onGone)
    handle.addEventListener("error", onGone)
  }

  startWorker()

  async function requestAs<T>(frame: BrokerRequest, parse: (value: unknown) => T): Promise<T> {
    return parse(await request(frame))
  }

  const acquire = (input: AcquireInput): Promise<AcquireResult> =>
    requestAs({ kind: "acquire", requestId: nextId(), directory: input.directory, servers: input.servers }, parseAcquireResult)

  const call = (input: CallInput): Promise<unknown> =>
    request({
      kind: "call",
      requestId: input.requestId ?? nextId(),
      directory: input.directory,
      server: input.server,
      method: input.method,
      ...(input.params === undefined ? {} : { params: input.params }),
      ...(input.timeoutMs === undefined ? {} : { timeoutMs: input.timeoutMs }),
    })

  const cancel = (targetId: string): Promise<void> =>
    request({ kind: "cancel", requestId: nextId(), targetId }).then(() => undefined)

  const oauth = (directory: string): Promise<OAuthResult> =>
    requestAs({ kind: "oauth", requestId: nextId(), directory }, parseOAuthResult)

  const release = (directory: string): Promise<void> =>
    request({ kind: "release", requestId: nextId(), directory }).then(() => undefined)

  const ping = (): Promise<PingResult> => requestAs({ kind: "ping", requestId: nextId() }, parsePingResult)

  async function teardown(): Promise<void> {
    if (disposed) return
    disposed = true
    if (alive && port) {
      const activePort = port
      await new Promise<void>((resolve) => {
        const id = nextId()
        const finish = () => {
          clearTimeout(timer)
          pending.delete(id)
          resolve()
        }
        const timer = setTimeout(finish, 1500)
        pending.set(id, { resolve: () => finish(), reject: () => finish(), timer })
        activePort.postMessage({ kind: "teardown", requestId: id } satisfies BrokerRequest)
      })
    }
    alive = false
    rejectPending(new BrokerRequestError("crashed", "broker torn down"))
    for (const child of mirror.values()) reaper.reap(child.pgid)
    mirror.clear()
    port?.close()
    port = undefined
    await Promise.resolve(worker?.terminate()).catch(() => undefined)
  }

  const onNotification = (handler: (notification: NotificationLike) => void): (() => void) => {
    notificationHandlers.add(handler)
    return () => notificationHandlers.delete(handler)
  }

  const onSupervision = (handler: (event: SupervisionEvent) => void): (() => void) => {
    supervisionHandlers.add(handler)
    return () => supervisionHandlers.delete(handler)
  }

  return {
    ready,
    get generation() {
      return generation
    },
    get threadId() {
      return threadId
    },
    get alive() {
      return alive
    },
    transport: "message-port",
    get handshake() {
      return handshake.slice()
    },
    workerHandle,
    portHandle,
    get children() {
      return [...mirror.values()]
    },
    acquire,
    call,
    cancel,
    oauth,
    release,
    ping,
    teardown,
    onNotification,
    onSupervision,
    [Symbol.asyncDispose]: teardown,
  }
}

export * as Broker from "./broker"
