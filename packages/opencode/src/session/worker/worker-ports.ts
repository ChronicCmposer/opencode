// Worker data-port vocabulary and the hub-side attach/dispose/serve registry.
import { createEgressReceiver } from "@/session/egress/receiver"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Layer, SynchronizedRef, type Scope } from "effect"
import { type PidReport, type PidReportEntry } from "./pid-registry"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { noopWorkerLogger, workerLogMessage, type WorkerLogger } from "./log"

/**
 * The worker data ports transferred as one `{type:"ports", …}` frame
 * (session-worker.ts). The writer port is intentionally absent: the hub serves no
 * SQLite RPC (D5).
 */
export interface WorkerTransferredPorts {
  readonly egress?: MessagePort
  readonly egressControl?: MessagePort
}

/** The data ports present in a transfer, in a stable order (for diagnostics). */
const TRANSFERRED_PORT_NAMES = ["egress", "egressControl"] as const
function transferredPortNames(ports: WorkerTransferredPorts): string[] {
  return TRANSFERRED_PORT_NAMES.filter((name) => ports[name] !== undefined)
}

/**
 * Whether `frame` is a well-formed {@link PidReport} (fail-closed parse). A pid
 * report is a plain data frame carried on the worker control (hub-proxy) port,
 * recognised by its `type` rather than the hub-proxy `kind` discriminator.
 */
export function isPidReport(frame: unknown): frame is PidReport {
  if (typeof frame !== "object" || frame === null) return false
  const candidate = frame as Partial<PidReport>
  if (candidate.type !== "pid-report") return false
  if (typeof candidate.workerID !== "number") return false
  if (!Array.isArray(candidate.entries)) return false
  return candidate.entries.every(
    (entry) =>
      typeof entry === "object" &&
      entry !== null &&
      typeof (entry as PidReportEntry).pid === "number" &&
      typeof (entry as PidReportEntry).pgid === "number" &&
      typeof (entry as PidReportEntry).label === "string" &&
      typeof (entry as PidReportEntry).workerID === "number",
  )
}

/**
 * The hub-resident authorities a worker's ports are served against. `egress`
 * defaults to a draining receiver (the SSE route wires a real sink later);
 * `hubProxy` is served over the worker control port when present. There is
 * deliberately no writer RPC (D5) and no broker authority (D20).
 */
export interface WorkerPortBackends {
  /** Serve a worker's egress port; defaults to a draining receiver. */
  readonly egress?: (workerID: number, port: MessagePort, control?: MessagePort) => () => void
  /** Handlers for the three hub-proxy authorities served over the control port. */
  readonly hubProxy?: HubProxyHandlers
  /**
   * Per-worker hub-proxy handlers. A worker is bound to exactly ONE session, so
   * the production backend builds each control port's handler map under that
   * worker's session context (never a first-dispatch latch). Takes precedence
   * over {@link hubProxy} when both are present.
   */
  readonly hubProxyFor?: (workerID: number) => HubProxyHandlers | undefined
  /**
   * Per-worker streaming hub-proxy handlers: the methods that may push correlated
   * events (e.g. `pluginTool.execute`'s metadata) to the awaiting worker before
   * replying. Served alongside {@link hubProxyFor}; takes precedence over
   * {@link hubProxyStreaming}.
   */
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  /** Streaming hub-proxy handlers shared by every worker (fallback for {@link hubProxyStreamingFor}). */
  readonly hubProxyStreaming?: Readonly<Record<string, HubProxyStreamHandler>>
  /**
   * Tear down the backends' own resources. Called once by
   * `SessionHub.terminateAll`, only when the backends were actually built.
   */
  readonly dispose?: () => void | Promise<void>
}

/**
 * The hub's port authorities, either ready to use or built on demand. A source
 * function is invoked at most once, on the first worker port attach (i.e. on the
 * first dispatch), so building a production node does no work at graph-build
 * time.
 */
export type WorkerPortBackendsSource = WorkerPortBackends | (() => WorkerPortBackends)

/**
 * Serve a worker control port for `{type:"pid-report"}` frames, forwarding each
 * well-formed frame to `onReport`. A pid report is a plain data frame, while the
 * hub-proxy server on the same port decodes JSON envelopes: consuming the raw
 * frame here and stopping its propagation keeps the two servers coexisting on
 * one control port. Returns a disposer (the caller's port stays open).
 */
export function servePidReportPort(onReport: (frame: PidReport) => void, port: MessagePort): () => void {
  const listener = (event: MessageEvent) => {
    if (!isPidReport(event.data)) return
    event.stopImmediatePropagation()
    onReport(event.data)
  }
  port.addEventListener("message", listener)
  port.start()
  return () => port.removeEventListener("message", listener)
}

/** Default egress serving: drain the worker's frames (the SSE route wires a real sink later). */
export function drainEgressPort(port: MessagePort): () => void {
  const receiver = createEgressReceiver({ port, onFrame: () => {} })
  return () => receiver.close()
}

/**
 * The per-worker port servers: the transferred egress and hub-proxy
 * control ports. Each attach is idempotent per worker (a re-attach disposes the
 * previous server first), and `disposeAll` tears every server down. The backend
 * source and handler map are thunks because both materialise lazily, after the
 * pool and dispatcher exist.
 */
export interface WorkerPortRegistryOptions {
  readonly log: WorkerLogger
  readonly backends: () => WorkerPortBackends | undefined
  /** The handler map for one worker's control port (per-session context). */
  readonly hubProxyHandlersFor: (workerID: number) => HubProxyHandlers | undefined
  /** The streaming half of one worker's control port (e.g. `pluginTool.metadata`). */
  readonly hubProxyStreamingFor: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

export interface WorkerPortRegistry {
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => () => void
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => () => void
  /**
   * Dispose one retired worker's port servers (egress and hub-proxy).
   * Idempotent; a never-attached worker is a no-op.
   */
  readonly disposeWorker: (workerID: number) => void
  readonly disposeAll: () => void
}

// ---------------------------------------------------------------------------
// Effect service shape.
//
// The `WorkerPorts` service is the same registry behavior expressed as an
// Effect service. It owns the disposer maps and backend memoization in
// `SynchronizedRef`s; `createWorkerPortRegistry` stays the synchronous adapter
// over the shared server builders below.
// ---------------------------------------------------------------------------

/** The workers with an attached data-port server and/or hub-proxy server. */
export interface WorkerPortsDiagnostics {
  readonly workerPorts: readonly number[]
  readonly hubProxy: readonly number[]
}

export interface WorkerPorts {
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => Effect.Effect<() => void>
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => Effect.Effect<() => void>
  /** Dispose one retired worker's port servers. Idempotent. */
  readonly disposeWorker: (workerID: number) => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
  /** Which workers currently hold a data-port and/or hub-proxy server. */
  readonly attached: () => Effect.Effect<WorkerPortsDiagnostics>
}

export class Service extends Context.Service<Service, WorkerPorts>()("@opencode/WorkerPorts") {}

/**
 * Construction options for the service. Unlike the synchronous registry, the
 * backend source and the per-worker handler seams are optional: when no
 * per-worker seam is supplied the backends' own handlers are the fallback.
 */
export interface WorkerPortsOptions {
  readonly log?: WorkerLogger
  readonly backends?: WorkerPortBackendsSource
  readonly hubProxyHandlersFor?: (workerID: number) => HubProxyHandlers | undefined
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

type WorkerPortServerOptions = Pick<WorkerPortsOptions, "hubProxyHandlersFor" | "hubProxyStreamingFor" | "onPidReport">

/** Run each disposer at most once, even when a returned/stored disposer is reused. */
function idempotentDisposer(disposers: readonly (() => void)[]): () => void {
  let disposed = false
  return () => {
    if (disposed) return
    disposed = true
    for (const dispose of disposers) dispose()
  }
}

/**
 * Build a worker's data-port server. Returns `undefined` when the hub has no
 * backend for the transferred port (the worker only transfers ports the hub
 * advertised in `capabilities.sessionWorkerPorts`).
 */
function buildWorkerPortServer(
  workerID: number,
  ports: WorkerTransferredPorts,
  backends: WorkerPortBackends | undefined,
): (() => void) | undefined {
  if (backends === undefined) return undefined
  if (ports.egress === undefined) return undefined
  const dispose =
    backends.egress !== undefined
      ? backends.egress(workerID, ports.egress, ports.egressControl)
      : drainEgressPort(ports.egress)
  return idempotentDisposer([dispose])
}

/**
 * Build a worker's control-port servers: the hub-proxy request server plus the
 * pid-report listener. The pid listener is installed first and consumes its raw
 * frame, so the JSON-enveloped hub-proxy decoder never sees it.
 */
function buildHubProxyServer(
  workerID: number,
  port: MessagePort,
  options: WorkerPortServerOptions,
  backends: WorkerPortBackends | undefined,
): (() => void) | undefined {
  const disposers: (() => void)[] = []

  const onPidReport = options.onPidReport
  if (onPidReport !== undefined) {
    disposers.push(servePidReportPort((report) => onPidReport(workerID, report), port))
  }

  const handlers =
    options.hubProxyHandlersFor !== undefined
      ? options.hubProxyHandlersFor(workerID)
      : (backends?.hubProxyFor?.(workerID) ?? backends?.hubProxy)
  if (handlers !== undefined) {
    const streaming =
      options.hubProxyStreamingFor !== undefined
        ? options.hubProxyStreamingFor(workerID)
        : (backends?.hubProxyStreamingFor?.(workerID) ?? backends?.hubProxyStreaming)
    disposers.push(serveHubProxy(handlers, port, streaming))
  }

  if (disposers.length === 0) return undefined
  return idempotentDisposer(disposers)
}

function logWorkerPorts(log: WorkerLogger, workerID: number, ports: WorkerTransferredPorts): void {
  log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: transferredPortNames(ports) })
}

function logHubProxyPort(log: WorkerLogger, workerID: number): void {
  log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: ["hub-proxy"] })
}

/** Memoise a synchronous backend source so it is invoked at most once. */
function memoizedBackends(
  source: WorkerPortBackends | (() => WorkerPortBackends | undefined) | undefined,
): () => WorkerPortBackends | undefined {
  if (source === undefined) return () => undefined
  if (typeof source !== "function") return () => source
  let resolved = false
  let value: WorkerPortBackends | undefined
  return () => {
    if (resolved) return value
    value = source()
    resolved = true
    return value
  }
}

export function createWorkerPortRegistry(options: WorkerPortRegistryOptions): WorkerPortRegistry {
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()
  const backends = memoizedBackends(options.backends)

  const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts): (() => void) => {
    logWorkerPorts(options.log, workerID, ports)
    workerPortDisposers.get(workerID)?.()
    const next = buildWorkerPortServer(workerID, ports, backends())
    if (next === undefined) {
      workerPortDisposers.delete(workerID)
      return () => {}
    }
    workerPortDisposers.set(workerID, next)
    return next
  }

  const attachHubProxyPort = (workerID: number, port: MessagePort): (() => void) => {
    logHubProxyPort(options.log, workerID)
    hubProxyDisposers.get(workerID)?.()
    const next = buildHubProxyServer(workerID, port, options, backends())
    if (next === undefined) {
      hubProxyDisposers.delete(workerID)
      return () => {}
    }
    hubProxyDisposers.set(workerID, next)
    return next
  }

  return {
    attachWorkerPorts,
    attachHubProxyPort,
    disposeWorker: (workerID) => {
      workerPortDisposers.get(workerID)?.()
      hubProxyDisposers.get(workerID)?.()
    },
    disposeAll: () => {
      for (const dispose of [...workerPortDisposers.values()]) dispose()
      for (const dispose of [...hubProxyDisposers.values()]) dispose()
    },
  }
}

type BackendResolution =
  | { readonly resolved: false }
  | { readonly resolved: true; readonly value: WorkerPortBackends | undefined }

const unresolvedBackends: BackendResolution = { resolved: false }

/**
 * Construct the `WorkerPorts` service. The backend source and every disposer
 * map live in `SynchronizedRef`s, so a concurrent first attach resolves the
 * backends exactly once and disposer swaps are serialized.
 */
export function make(options: WorkerPortsOptions = {}): Effect.Effect<WorkerPorts, never, Scope.Scope> {
  return Effect.gen(function* () {
    const log = options.log ?? noopWorkerLogger
    const workerPortDisposers = yield* SynchronizedRef.make(new Map<number, () => void>())
    const hubProxyDisposers = yield* SynchronizedRef.make(new Map<number, () => void>())
    const backendResolution = yield* SynchronizedRef.make(unresolvedBackends)

    const resolveBackends = Effect.gen(function* () {
      const current = yield* SynchronizedRef.get(backendResolution)
      if (current.resolved) return current.value
      return yield* SynchronizedRef.modifyEffect(backendResolution, (state) =>
        state.resolved
          ? Effect.succeed([state.value, state] as const)
          : Effect.sync(() => {
              const value = typeof options.backends === "function" ? options.backends() : options.backends
              return [value, { resolved: true, value } as const] as const
            }),
      )
    })

    const detach = (ref: SynchronizedRef.SynchronizedRef<Map<number, () => void>>, workerID: number) =>
      SynchronizedRef.modifyEffect(ref, (current) =>
        Effect.sync(() => {
          current.get(workerID)?.()
          const next = new Map(current)
          next.delete(workerID)
          return [undefined, next] as const
        }),
      )

    /**
     * The disposer returned to a caller also forgets its own map entry, but only
     * while that entry is still current: a later re-attach owns the slot and must
     * not be removed by the superseded disposer.
     */
    const attachedDisposer = (
      ref: SynchronizedRef.SynchronizedRef<Map<number, () => void>>,
      workerID: number,
      disposer: () => void,
    ): (() => void) => {
      let released = false
      return () => {
        if (released) return
        released = true
        disposer()
        Effect.runSync(
          SynchronizedRef.update(ref, (current) => {
            if (current.get(workerID) !== disposer) return current
            const next = new Map(current)
            next.delete(workerID)
            return next
          }),
        )
      }
    }

    const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts) =>
      Effect.gen(function* () {
        logWorkerPorts(log, workerID, ports)
        const next = buildWorkerPortServer(workerID, ports, yield* resolveBackends)
        if (next === undefined) {
          yield* detach(workerPortDisposers, workerID)
          return () => {}
        }
        yield* SynchronizedRef.modifyEffect(workerPortDisposers, (current) =>
          Effect.sync(() => {
            current.get(workerID)?.()
            return [undefined, new Map(current).set(workerID, next)] as const
          }),
        )
        return attachedDisposer(workerPortDisposers, workerID, next)
      })

    const attachHubProxyPort = (workerID: number, port: MessagePort) =>
      Effect.gen(function* () {
        logHubProxyPort(log, workerID)
        const next = buildHubProxyServer(workerID, port, options, yield* resolveBackends)
        if (next === undefined) {
          yield* detach(hubProxyDisposers, workerID)
          return () => {}
        }
        yield* SynchronizedRef.modifyEffect(hubProxyDisposers, (current) =>
          Effect.sync(() => {
            current.get(workerID)?.()
            return [undefined, new Map(current).set(workerID, next)] as const
          }),
        )
        return attachedDisposer(hubProxyDisposers, workerID, next)
      })

    const disposeWorker = (workerID: number) =>
      Effect.gen(function* () {
        yield* detach(workerPortDisposers, workerID)
        yield* detach(hubProxyDisposers, workerID)
      })

    const disposeAll = () =>
      Effect.gen(function* () {
        yield* SynchronizedRef.modifyEffect(workerPortDisposers, (current) =>
          Effect.sync(() => {
            for (const dispose of current.values()) dispose()
            return [undefined, new Map()] as const
          }),
        )
        yield* SynchronizedRef.modifyEffect(hubProxyDisposers, (current) =>
          Effect.sync(() => {
            for (const dispose of current.values()) dispose()
            return [undefined, new Map()] as const
          }),
        )
      })

    const attached = () =>
      Effect.gen(function* () {
        const [ports, proxies] = yield* Effect.all([
          SynchronizedRef.get(workerPortDisposers),
          SynchronizedRef.get(hubProxyDisposers),
        ])
        return {
          workerPorts: [...ports.keys()].sort((left, right) => left - right),
          hubProxy: [...proxies.keys()].sort((left, right) => left - right),
        }
      })

    const ports: WorkerPorts = { attachWorkerPorts, attachHubProxyPort, disposeWorker, disposeAll, attached }
    yield* Effect.addFinalizer(() => ports.disposeAll())
    return ports
  })
}

export function layer(options: WorkerPortsOptions = {}): Layer.Layer<Service> {
  return Layer.effect(Service, make(options))
}

export function node(options: WorkerPortsOptions = {}): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(options), deps: [] })
}

export * as WorkerPorts from "./worker-ports"
