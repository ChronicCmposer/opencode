// Worker data-port vocabulary and the hub-side attach/dispose/serve registry.
import { createEgressReceiver } from "@/session/egress/receiver"
import { type PidReport, type PidReportEntry } from "./pid-registry"
import type { HubProxyHandlers, HubProxyStreamHandler } from "./hub-proxy-wire"
import { serveHubProxy } from "./hub-proxy-wire"
import { portFromMessagePort } from "./port"
import { workerLogMessage, type WorkerLogger } from "./log"

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
 * well-formed frame to `onReport`. `portFromMessagePort` registers an
 * `addEventListener` listener, so this coexists with the hub-proxy request
 * server on the same port. Returns a disposer.
 */
export function servePidReportPort(onReport: (frame: PidReport) => void, port: MessagePort): () => void {
  const channel = portFromMessagePort<unknown>(port)
  return channel.onMessage((frame) => {
    if (isPidReport(frame)) onReport(frame)
  })
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
interface WorkerPortRegistryOptions {
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

export function createWorkerPortRegistry(options: WorkerPortRegistryOptions): WorkerPortRegistry {
  const { log } = options
  const workerPortDisposers = new Map<number, () => void>()
  const hubProxyDisposers = new Map<number, () => void>()

  // Serve a worker's transferred egress ports against the hub authorities. A
  // port the hub has no backend for is left unserved (the worker only transfers
  // ports the hub advertised in `capabilities.sessionWorkerPorts`).
  const attachWorkerPorts = (workerID: number, ports: WorkerTransferredPorts): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: transferredPortNames(ports) })
    workerPortDisposers.get(workerID)?.()
    const disposers: (() => void)[] = []
    const backends = options.backends()
    if (backends !== undefined) {
      if (ports.egress !== undefined) {
        disposers.push(
          backends.egress !== undefined
            ? backends.egress(workerID, ports.egress, ports.egressControl)
            : drainEgressPort(ports.egress),
        )
      }
    }
    const dispose = () => {
      for (const disposer of disposers) disposer()
      workerPortDisposers.delete(workerID)
    }
    workerPortDisposers.set(workerID, dispose)
    return dispose
  }

  // Serve the hub-proxy authorities over a worker's control port, plus any
  // `{type:"pid-report"}` frames the worker sends on the same port.
  const attachHubProxyPort = (workerID: number, port: MessagePort): (() => void) => {
    log(workerLogMessage("ports connected"), { level: "INFO", workerID, ports: ["hub-proxy"] })
    hubProxyDisposers.get(workerID)?.()
    const disposers: (() => void)[] = []
    const handlers = options.hubProxyHandlersFor(workerID)
    if (handlers !== undefined) disposers.push(serveHubProxy(handlers, port, options.hubProxyStreamingFor(workerID)))
    const onPidReport = options.onPidReport
    if (onPidReport !== undefined) {
      // The port association is the authoritative owner: the worker only knows
      // its own thread id, not the hub's slot id.
      disposers.push(servePidReportPort((report) => onPidReport(workerID, report), port))
    }
    if (disposers.length === 0) return () => {}
    const dispose = () => {
      for (const disposer of disposers) disposer()
      hubProxyDisposers.delete(workerID)
    }
    hubProxyDisposers.set(workerID, dispose)
    return dispose
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
