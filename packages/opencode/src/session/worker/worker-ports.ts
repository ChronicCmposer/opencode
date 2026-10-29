// Worker data-port vocabulary and the hub-side attach/dispose service.
import { createEgressReceiver } from "@/session/egress/receiver"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Context, Effect, Layer, Option, Schema, type Scope } from "effect"
import type { EffectBridge } from "@/effect/bridge"
import { type PidReport } from "./pid-registry"
import { WorkerID } from "./frame"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { workerLogMessage } from "./worker-logging"


/**
 * The worker data ports transferred as one `{type:"ports", …}` frame
 * (session-worker.ts). The writer port is intentionally absent: the hub serves no
 * SQLite RPC (D5).
 */
export interface WorkerTransferredPorts {
  readonly egress?: MessagePort
  readonly egressControl?: MessagePort
}

const TRANSFERRED_PORT_NAMES = ["egress", "egressControl"] as const
function transferredPortNames(ports: WorkerTransferredPorts): string[] {
  return TRANSFERRED_PORT_NAMES.filter((name) => ports[name] !== undefined)
}

const PidReportSchema = Schema.Struct({
  type: Schema.Literal("pid-report"),
  workerID: WorkerID,
  entries: Schema.Array(
    Schema.Struct({
      pid: Schema.Int,
      pgid: Schema.Int,
      label: Schema.String,
      workerID: WorkerID,
    }),
  ),
})

const decodePidReport = Schema.decodeUnknownOption(PidReportSchema)

/**
 * Whether `frame` is a well-formed {@link PidReport}, decoded once with Schema at
 * the port boundary. A pid report is a plain data frame carried on the worker
 * control (hub-proxy) port, recognised by its `type` rather than the hub-proxy
 * `kind` discriminator.
 */
export function isPidReport(frame: unknown): frame is PidReport {
  return Option.isSome(decodePidReport(frame))
}

/**
 * The hub-resident authorities a worker's ports are served against. `egress`
 * defaults to a draining receiver; `hubProxy` is served over the worker control
 * port when present. There is deliberately no writer RPC (D5) and no broker
 * authority (D20).
 */
export interface WorkerPortBackends {
  readonly egress?: (workerID: number, port: MessagePort, control?: MessagePort) => () => void
  readonly hubProxy?: HubProxyHandlers
  readonly hubProxyFor?: (workerID: number) => HubProxyHandlers | undefined
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly hubProxyStreaming?: Readonly<Record<string, HubProxyStreamHandler>>
  readonly dispose?: () => void | Promise<void>
}

export type WorkerPortBackendsSource = WorkerPortBackends | (() => WorkerPortBackends | undefined)

/**
 * Serve a worker control port for `{type:"pid-report"}` frames, forwarding each
 * well-formed frame to `onReport`. The raw frame is consumed here and its
 * propagation stopped so the JSON-enveloped hub-proxy server on the same port
 * never sees it. Returns a disposer (the caller's port stays open).
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

/** The synchronous port surface the Promise-shaped hub consumes. */
export interface Attachments {
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => () => void
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => () => void
  readonly disposeWorker: (workerID: number) => void
  readonly disposeAll: () => void
  readonly attached: () => WorkerPortsDiagnostics
}

/** The workers with an attached data-port server and/or hub-proxy server. */
export interface WorkerPortsDiagnostics {
  readonly workerPorts: readonly number[]
  readonly hubProxy: readonly number[]
}

/**
 * The worker-port authority: it owns the attached port servers for this hub and
 * the synchronous {@link Attachments} facade the hub's Promise surface consumes.
 * Configuration is read when the layer is built; there is no per-call options
 * bag.
 *
 * The events port has ONE live implementation: the hub's `serveEventsPort`
 * (`session/worker/hub.ts`), a decoded `Stream.callback` pipeline driven by the
 * hub's `EffectBridge` from this synchronous attach facade. It owns durable
 * ordering, gap healing and backpressure; there is deliberately no second
 * stream-shaped events surface here to drift from it.
 */
export interface Interface {
  readonly attachWorkerPorts: (workerID: number, ports: WorkerTransferredPorts) => Effect.Effect<() => void>
  readonly attachHubProxyPort: (workerID: number, port: MessagePort) => Effect.Effect<() => void>
  readonly disposeWorker: (workerID: number) => Effect.Effect<void>
  readonly disposeAll: () => Effect.Effect<void>
  readonly attached: () => Effect.Effect<WorkerPortsDiagnostics>
  /** The synchronous attach surface the Promise hub composes. */
  readonly attachments: Attachments
}

export class Service extends Context.Service<Service, Interface>()("@opencode/WorkerPorts") {}
export type WorkerPorts = Interface

export type WorkerPortsOptions = Options

export interface Options {
  readonly bridge?: EffectBridge.Shape
  readonly backends?: WorkerPortBackendsSource
  readonly hubProxyHandlersFor?: (workerID: number) => HubProxyHandlers | undefined
  readonly hubProxyStreamingFor?: (workerID: number) => Readonly<Record<string, HubProxyStreamHandler>> | undefined
  readonly onPidReport?: (workerID: number, report: PidReport) => void
}

type WorkerPortServerOptions = Pick<Options, "hubProxyHandlersFor" | "hubProxyStreamingFor" | "onPidReport">

function idempotentDisposer(disposers: readonly (() => void)[]): () => void {
  let disposed = false
  return () => {
    if (disposed) return
    disposed = true
    for (const dispose of disposers) dispose()
  }
}

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

function logWorkerPorts(bridge: EffectBridge.Shape | undefined, workerID: number, ports: WorkerTransferredPorts): void {
  void bridge?.fork(Effect.logInfo(workerLogMessage("ports connected"), { workerID, ports: transferredPortNames(ports) }))
}

function logHubProxyPort(bridge: EffectBridge.Shape | undefined, workerID: number): void {
  void bridge?.fork(Effect.logInfo(workerLogMessage("ports connected"), { workerID, ports: ["hub-proxy"] }))
}

function memoizedBackends(source: WorkerPortBackendsSource | undefined): () => WorkerPortBackends | undefined {
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

/**
 * Build the synchronous attach authority. The layer and a direct hub both call
 * this one implementation; it owns the per-worker disposer maps.
 */
export function makeAttachments(options: Options = {}): Attachments {
  const bridge = options.bridge
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()
  const backends = memoizedBackends(options.backends)

  const trackedDisposer = (map: Map<number, () => void>, workerID: number, disposer: () => void): (() => void) => {
    let released = false
    return () => {
      if (released) return
      released = true
      disposer()
      if (map.get(workerID) === disposer) map.delete(workerID)
    }
  }

  const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts): (() => void) => {
    logWorkerPorts(bridge, workerID, ports)
    workerPortDisposers.get(workerID)?.()
    const next = buildWorkerPortServer(workerID, ports, backends())
    if (next === undefined) {
      workerPortDisposers.delete(workerID)
      return () => {}
    }
    workerPortDisposers.set(workerID, next)
    return trackedDisposer(workerPortDisposers, workerID, next)
  }

  const attachHubProxyPort = (workerID: number, port: MessagePort): (() => void) => {
    logHubProxyPort(bridge, workerID)
    hubProxyDisposers.get(workerID)?.()
    const next = buildHubProxyServer(workerID, port, options, backends())
    if (next === undefined) {
      hubProxyDisposers.delete(workerID)
      return () => {}
    }
    hubProxyDisposers.set(workerID, next)
    return trackedDisposer(hubProxyDisposers, workerID, next)
  }

  return {
    attachWorkerPorts,
    attachHubProxyPort,
    disposeWorker: (workerID) => {
      workerPortDisposers.get(workerID)?.()
      hubProxyDisposers.get(workerID)?.()
      workerPortDisposers.delete(workerID)
      hubProxyDisposers.delete(workerID)
    },
    disposeAll: () => {
      for (const dispose of [...workerPortDisposers.values()]) dispose()
      for (const dispose of [...hubProxyDisposers.values()]) dispose()
      workerPortDisposers.clear()
      hubProxyDisposers.clear()
    },
    attached: () => ({
      workerPorts: [...workerPortDisposers.keys()].sort((left, right) => left - right),
      hubProxy: [...hubProxyDisposers.keys()].sort((left, right) => left - right),
    }),
  }
}

const makeService = Effect.fn("WorkerPorts.make")(function* (
  options: Options,
): Effect.fn.Return<Interface, never, Scope.Scope> {
  const attachments = makeAttachments(options)
  const attachWorkerPorts = Effect.fn("WorkerPorts.attachWorkerPorts")(function* (
    workerID: number,
    ports: WorkerTransferredPorts,
  ) {
    return attachments.attachWorkerPorts(workerID, ports)
  })
  const attachHubProxyPort = Effect.fn("WorkerPorts.attachHubProxyPort")(function* (
    workerID: number,
    port: MessagePort,
  ) {
    return attachments.attachHubProxyPort(workerID, port)
  })
  const disposeWorker = Effect.fn("WorkerPorts.disposeWorker")(function* (workerID: number) {
    attachments.disposeWorker(workerID)
  })
  const disposeAll = Effect.fn("WorkerPorts.disposeAll")(function* () {
    attachments.disposeAll()
  })
  const attached = Effect.fn("WorkerPorts.attached")(function* () {
    return attachments.attached()
  })
  const service: Interface = {
    attachWorkerPorts,
    attachHubProxyPort,
    disposeWorker,
    disposeAll,
    attached,
    attachments,
  }
  yield* Effect.addFinalizer(() => disposeAll())
  return service
})

export function make(options: Options = {}): Effect.Effect<Interface, never, Scope.Scope> {
  return makeService(options)
}

export function layer(options: Options = {}): Layer.Layer<Service> {
  return Layer.effect(Service, make(options))
}

export function node(options: Options = {}): LayerNode.Node<Service, never, undefined> {
  return LayerNode.make({ service: Service, layer: layer(options), deps: [] })
}

export * as WorkerPorts from "./worker-ports"
