// Phase 10 — the MCP/LSP broker worker (plan tasks 10.1–10.8).
//
// This is a dedicated Bun Worker thread — NOT one of the K session workers and
// NOT headroom. It owns every MCP/LSP child process so that N session workers
// share exactly one set of children per directory. All hub↔broker traffic rides
// a transferred `MessagePort` (see `broker.ts`), keeping the hub loop free of
// child-process I/O.
//
// Responsibilities, in one place:
//   - per-directory `InstanceState` (a stable instance id + OAuth callback),
//   - per-directory serialization (MCP servers are often not concurrency-safe),
//   - `mcp_timeout` enforcement on the handshake and on every call,
//   - PGID ownership: children are spawned detached as group leaders; teardown
//     group-kills them (never per-PID, unlike `src/mcp/index.ts`'s finalizer),
//   - notification fan-out to the hub,
//   - cancel propagation by request id.
//
// Like `bootstrap.ts` this module has top-level side effects and is meant to be
// loaded only as a Worker entry point; import its types from `protocol.ts`.

import { randomUUID } from "node:crypto"
import { existsSync, statSync } from "node:fs"
import { threadId } from "node:worker_threads"
import { Faults } from "@/testing/faults"
import {
  type AcquireResult,
  type AcquiredServer,
  type BrokerError,
  type BrokerFrame,
  type BrokerRequest,
  type BrokerWorkerInbound,
  type CallRequest,
  type ServerSpec,
} from "./protocol"
import {
  BrokerFailure,
  parseBrokerRequest,
  parseWorkerAttach,
  rememberBounded,
  resolveRequestTimeout,
  toBrokerError,
} from "./protocol"
import { mergeEnv } from "./env"
import { type Framer, framerFor } from "./framer"
import { isRecord } from "@/session/worker/frame"
import { portFromMessagePort, rawRequestId, servePort } from "@/session/worker/port"
import { setThreadName } from "@/util/thread-name"

// Name this isolate's OS thread for human observability (Linux-only, fail-soft).
setThreadName("ocb")

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

function reapGroup(pgid: number, signal: NodeJS.Signals = "SIGTERM"): void {
  try {
    process.kill(-pgid, signal)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
      /* best-effort teardown */
    }
  }
}

// ---------------------------------------------------------------------------
// Child processes
// ---------------------------------------------------------------------------

interface PendingReply {
  readonly settleResolve: (value: unknown) => void
  readonly settleReject: (error: BrokerFailure) => void
}

interface ChildHandle {
  readonly directory: string
  readonly spec: ServerSpec
  readonly process: Bun.Subprocess
  readonly pid: number
  readonly pgid: number
  readonly framer: Framer
  readonly pending: Map<number, PendingReply>
  /** Resolves with the exit code once this isolate has reaped the child. */
  readonly exited: Promise<number>
  nextId: number
  /** Serializes stdin writes (and their flush) so frames never interleave. */
  stdinChain: Promise<void>
  /** True once teardown or the exit watcher has claimed cleanup. */
  stopped: boolean
  exitEmitted: boolean
}

interface ActiveCall {
  readonly child: ChildHandle
  readonly abort: (error: BrokerFailure) => void
}

interface DirectoryState {
  readonly directory: string
  readonly instanceId: string
  readonly children: Map<string, ChildHandle>
  /** Serialization chain: only one call per directory runs at a time. */
  queue: Promise<void>
  oauthCallback: string | null
}

const directories = new Map<string, DirectoryState>()
const activeCalls = new Map<string, ActiveCall>()
/** Calls cancelled while still queued (not yet dispatched to a child). */
const cancelled = new Set<string>()

let rpc: MessagePort | null = null
/** Set once teardown starts; queued work must not spawn/dispatch after it. */
let shuttingDown = false

function postFrame(frame: BrokerFrame): void {
  rpc?.postMessage(frame)
}

function writeChild(child: ChildHandle, text: string): void {
  const sink = child.process.stdin as Bun.FileSink | undefined
  if (!sink) return
  // Chain write → flush on the child's stdin queue: firing them independently
  // lets a later frame's write overtake an earlier frame's flush, reordering
  // frames on the wire (plan A15).
  child.stdinChain = child.stdinChain
    .then(() => sink.write(text))
    .then(() => sink.flush())
    .then(() => undefined)
    .catch(() => undefined)
}

function emitChildExit(child: ChildHandle, code: number | null): void {
  if (child.exitEmitted) return
  child.exitEmitted = true
  postFrame({
    kind: "child-exit",
    directory: child.directory,
    server: child.spec.name,
    pid: child.pid,
    pgid: child.pgid,
    code,
  })
}

function stopChild(child: ChildHandle): void {
  if (child.stopped) return
  child.stopped = true
  const error = new BrokerFailure("child-exited", `${child.spec.name} exited`)
  for (const pending of child.pending.values()) pending.settleReject(error)
  child.pending.clear()
  reapGroup(child.pgid)
  emitChildExit(child, null)
}

/** Resolve once every task settles, or after `timeoutMs`; always clears the timer. */
async function settleWithin(tasks: ReadonlyArray<Promise<unknown>>, timeoutMs: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, timeoutMs)
  })
  try {
    await Promise.race([Promise.allSettled(tasks).then(() => undefined), timeout])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

/** Wait (bounded) for a stopped child to be reaped by this isolate. */
async function awaitExit(child: ChildHandle, timeoutMs = 2000): Promise<void> {
  await settleWithin([child.exited], timeoutMs)
}

/** A decoded child message: a JSON-RPC reply (numeric `id`) or a notification/request (string `method`). */
type ChildMessage =
  | { readonly kind: "reply"; readonly id: number; readonly result: unknown; readonly error: unknown }
  | { readonly kind: "request"; readonly method: string; readonly params?: unknown }

/**
 * Guard-parse one decoded child message. A reply is identified by a numeric `id`,
 * a notification/request by a string `method`; anything else is not JSON-RPC and
 * is ignored. Parsing once keeps the dispatch below free of repeated `typeof`
 * checks and unsafe casts.
 */
function parseChildMessage(value: unknown): ChildMessage | undefined {
  if (!isRecord(value)) return undefined
  const id = value["id"]
  if (typeof id === "number") {
    return { kind: "reply", id, result: value["result"], error: value["error"] }
  }
  const method = value["method"]
  if (typeof method === "string") {
    return { kind: "request", method, ...(value["params"] === undefined ? {} : { params: value["params"] }) }
  }
  return undefined
}

function dispatchChildMessage(child: ChildHandle, message: unknown): void {
  const parsed = parseChildMessage(message)
  if (parsed === undefined) return
  if (parsed.kind === "reply") {
    // One lookup: a reply for a request we never sent (or already settled) is
    // dropped without a second `has`/`get` pair or a non-null assertion.
    const pending = child.pending.get(parsed.id)
    if (pending === undefined) return
    if (parsed.error !== undefined) pending.settleReject(new BrokerFailure("protocol", JSON.stringify(parsed.error)))
    else pending.settleResolve(parsed.result)
    return
  }
  postFrame({
    kind: "notification",
    directory: child.directory,
    server: child.spec.name,
    method: parsed.method,
    ...(parsed.params === undefined ? {} : { params: parsed.params }),
  })
}

/** Surface a framing/JSON error from a child's stdout as a broker notification. */
function surfaceFramerError(child: ChildHandle, error: string): void {
  postFrame({
    kind: "notification",
    directory: child.directory,
    server: child.spec.name,
    method: "notifications/broker",
    params: { level: "error", data: error },
  })
}

function pumpChild(child: ChildHandle): void {
  const stream = child.process.stdout as ReadableStream<Uint8Array>
  const reader = stream.getReader()
  void (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        const { messages, errors } = child.framer.push(value)
        for (const message of messages) dispatchChildMessage(child, message)
        for (const error of errors) surfaceFramerError(child, error)
      }
    } catch {
      /* the child is gone; exit handling owns cleanup */
    }
  })()
}

function spawnChild(directory: string, spec: ServerSpec): ChildHandle {
  // A missing directory is a caller error, not a reason to spawn the child in the
  // hub's working directory (which would silently run it in the wrong place).
  if (!existsDirectory(directory)) {
    throw new BrokerFailure("unknown-directory", `no directory ${directory}`)
  }
  const cwd = directory
  let process_: Bun.Subprocess
  try {
    process_ = Bun.spawn(spec.command as string[], {
      cwd,
      env: mergeEnv(spec.env),
      detached: true,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  } catch (error) {
    throw new BrokerFailure("spawn-failed", `failed to spawn ${spec.name}: ${(error as Error).message}`)
  }
  if (process_.pid === undefined) throw new BrokerFailure("spawn-failed", `failed to spawn ${spec.name}`)

  const child: ChildHandle = {
    directory,
    spec,
    process: process_,
    pid: process_.pid,
    pgid: process_.pid, // detached ⇒ its own group leader
    framer: framerFor(spec.kind),
    pending: new Map(),
    exited: process_.exited,
    nextId: 1,
    stdinChain: Promise.resolve(),
    stopped: false,
    exitEmitted: false,
  }

  pumpChild(child)

  void child.exited.then((code) => {
    if (child.stopped) return
    child.stopped = true
    const error = new BrokerFailure("child-exited", `${spec.name} exited with code ${code}`)
    for (const pending of child.pending.values()) pending.settleReject(error)
    child.pending.clear()
    // A naturally-exited child is dead: drop it from its directory so the next
    // acquire spawns a fresh one, and reap any descendants that outlived it.
    directories.get(directory)?.children.delete(spec.name)
    reapGroup(child.pgid)
    emitChildExit(child, code)
  })

  // Mirror the PGID to the hub BEFORE any handshake work, so the hub can reap
  // orphans even if this isolate dies mid-spawn.
  postFrame({ kind: "child", directory, server: spec.name, pid: child.pid, pgid: child.pgid })
  return child
}

function existsDirectory(directory: string): boolean {
  try {
    return existsSync(directory) && statSync(directory).isDirectory()
  } catch {
    return false
  }
}

interface DispatchHooks {
  /** Called once when the request settles (reply, reject, or timeout). */
  readonly onSettle?: () => void
  /** Called with the abort fn synchronously before the frame is written. */
  readonly onAbort?: (abort: (error: BrokerFailure) => void) => void
}

/**
 * Send one JSON-RPC request to a child and resolve with its reply (or reject on
 * timeout). This is the single implementation shared by the initialize
 * handshake and by call dispatch. `childId` and `timer` are declared before
 * `settle`, so no settle path can touch a TDZ binding.
 */
function dispatch(
  child: ChildHandle,
  method: string,
  params: unknown,
  timeoutMs: number,
  hooks: DispatchHooks = {},
): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    const childId = child.nextId++
    let done = false
    let timer!: ReturnType<typeof setTimeout>
    const settle = (finish: () => void) => {
      if (done) return
      done = true
      clearTimeout(timer)
      child.pending.delete(childId)
      hooks.onSettle?.()
      finish()
    }
    const abort = (error: BrokerFailure) => settle(() => reject(error))
    timer = setTimeout(() => abort(new BrokerFailure("timeout", `${method} timed out after ${timeoutMs}ms`)), timeoutMs)
    child.pending.set(childId, {
      settleResolve: (value) => settle(() => resolve(value)),
      settleReject: (error) => settle(() => reject(error)),
    })
    hooks.onAbort?.(abort)
    writeChild(child, child.framer.encode({ jsonrpc: "2.0", id: childId, method, ...(params === undefined ? {} : { params }) }))
  })
}

// ---------------------------------------------------------------------------
// Directory state
// ---------------------------------------------------------------------------

function directoryState(directory: string): DirectoryState {
  const existing = directories.get(directory)
  if (existing) return existing
  const created: DirectoryState = {
    directory,
    instanceId: randomUUID(),
    children: new Map(),
    queue: Promise.resolve(),
    oauthCallback: null,
  }
  directories.set(directory, created)
  return created
}

/** Serialize `task` behind any earlier task for the same directory. */
function runSerial<T>(state: DirectoryState, task: () => Promise<T>): Promise<T> {
  const prior = state.queue
  const run = prior.then(task, task)
  state.queue = run.then(
    () => undefined,
    () => undefined,
  )
  return run
}

// ---------------------------------------------------------------------------
// Request handlers
// ---------------------------------------------------------------------------

/** A failed initialize handshake is `handshake-failed`, except a real timeout. */
function asHandshakeFailure(name: string, error: unknown): BrokerFailure {
  if (error instanceof BrokerFailure && error.code === "timeout") return error
  const detail = error instanceof Error ? error.message : String(error)
  return new BrokerFailure("handshake-failed", `${name} initialize handshake failed: ${detail}`)
}

async function handleAcquire(request: BrokerRequest & { kind: "acquire" }): Promise<AcquireResult> {
  const state = directoryState(request.directory)
  // Serialize acquisitions per directory: two concurrent acquires of the same
  // (directory, server) must observe one child, not race to spawn two.
  return runSerial(state, async () => {
    const servers: AcquiredServer[] = []

    for (const spec of request.servers) {
      let child = state.children.get(spec.name)
      if (!child) {
        child = spawnChild(request.directory, spec)
        state.children.set(spec.name, child)
        try {
          await dispatch(
            child,
            "initialize",
            { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "opencode-broker", version: "1.0.0" } },
            resolveRequestTimeout(undefined, spec.timeoutMs),
          )
        } catch (error) {
          state.children.delete(spec.name)
          stopChild(child)
          await awaitExit(child)
          throw asHandshakeFailure(spec.name, error)
        }
      }
      servers.push({ name: spec.name, pid: child.pid, pgid: child.pgid, instanceId: state.instanceId })
    }

    return { instanceId: state.instanceId, servers }
  })
}

function dispatchCall(request: CallRequest, child: ChildHandle, timeoutMs: number): Promise<unknown> {
  return dispatch(child, request.method, request.params, timeoutMs, {
    onSettle: () => activeCalls.delete(request.requestId),
    onAbort: (abort) => activeCalls.set(request.requestId, { child, abort }),
  })
}

async function handleCall(request: CallRequest): Promise<unknown> {
  Faults.crashWorker("broker:call")

  const state = directories.get(request.directory)
  if (!state) throw new BrokerFailure("unknown-directory", `no directory ${request.directory}`)
  if (!state.children.has(request.server)) {
    throw new BrokerFailure("unknown-server", `no server ${request.server} in ${request.directory}`)
  }

  return runSerial(state, () => {
    // Re-resolve inside the serialized task: a release/teardown or a child exit
    // may have removed the directory or child while this call waited in the queue.
    if (shuttingDown) return Promise.reject(new BrokerFailure("crashed", "broker is shutting down"))
    const current = directories.get(request.directory)
    if (!current) return Promise.reject(new BrokerFailure("unknown-directory", `no directory ${request.directory}`))
    const child = current.children.get(request.server)
    if (!child) {
      return Promise.reject(
        new BrokerFailure("unknown-server", `no server ${request.server} in ${request.directory}`),
      )
    }
    if (cancelled.delete(request.requestId)) {
      return Promise.reject(new BrokerFailure("cancelled", `call ${request.requestId} cancelled before dispatch`))
    }
    return dispatchCall(request, child, resolveRequestTimeout(request.timeoutMs, child.spec.timeoutMs))
  })
}

function handleCancel(request: BrokerRequest & { kind: "cancel" }): void {
  const active = activeCalls.get(request.targetId)
  if (!active) {
    // Bound the queued-cancellation registry: an id that never matches a
    // dispatched call must not accumulate forever (plan A14).
    rememberBounded(cancelled, request.targetId)
    return
  }
  active.abort(new BrokerFailure("cancelled", `call ${request.targetId} cancelled`))
}

async function handleRelease(request: BrokerRequest & { kind: "release" }): Promise<void> {
  const state = directories.get(request.directory)
  if (!state) return
  const exits: Array<Promise<number>> = []
  for (const child of state.children.values()) {
    stopChild(child)
    exits.push(child.exited)
  }
  state.children.clear()
  directories.delete(request.directory)
  await settleWithin(exits, 2000)
}

function handleOAuth(request: BrokerRequest & { kind: "oauth" }) {
  const state = directoryState(request.directory)
  state.oauthCallback ??= `opencode://mcp/oauth/${state.instanceId}`
  return { callbackUrl: state.oauthCallback, instanceId: state.instanceId, owner: "broker" as const }
}

async function handleTeardown(): Promise<void> {
  shuttingDown = true
  const exits: Array<Promise<number>> = []
  for (const state of directories.values()) {
    for (const child of state.children.values()) {
      stopChild(child)
      exits.push(child.exited)
    }
    state.children.clear()
  }
  directories.clear()
  activeCalls.clear()
  cancelled.clear()
  // Reap every child inside this isolate BEFORE acknowledging teardown: a child
  // spawned from a worker thread becomes an unreapable zombie once the worker
  // is terminated (Bun 1.3.14 does not reap a dead worker's subprocesses).
  await settleWithin(exits, 2000)
}

function execute(request: BrokerRequest): unknown {
  switch (request.kind) {
    case "acquire":
      return handleAcquire(request)
    case "call":
      return handleCall(request)
    case "cancel":
      return handleCancel(request)
    case "release":
      return handleRelease(request)
    case "oauth":
      return handleOAuth(request)
    case "ping":
      return { threadId, directories: directories.size }
    case "teardown":
      return handleTeardown()
    default:
      throw new BrokerFailure("invalid-request", `unknown kind ${(request as { kind: string }).kind}`)
  }
}

function respond(requestId: string, value: unknown): void {
  postFrame({ kind: "response", requestId, ok: true, value })
}

function respondError(requestId: string, error: BrokerError): void {
  postFrame({ kind: "response", requestId, ok: false, error })
}

/** Serve the RPC port: one generic request/reply loop shared with the hub side. */
function serveRpc(port: MessagePort): void {
  servePort(
    portFromMessagePort<unknown>(port),
    parseBrokerRequest,
    execute,
    (raw, outcome) => {
      const requestId = outcome.request?.requestId ?? rawRequestId(raw)
      if (outcome.ok) respond(requestId, outcome.value ?? null)
      else respondError(requestId, toBrokerError(outcome.error))
    },
  )
}

// ---------------------------------------------------------------------------
// Worker entry
// ---------------------------------------------------------------------------

interface WorkerScope {
  postMessage(message: unknown): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as WorkerScope

scope.onmessage = (event) => {
  let message: BrokerWorkerInbound
  try {
    message = parseWorkerAttach(event.data)
  } catch {
    // The only inbound handshake frame is `attach`; anything else is a hub bug.
    return
  }
  rpc = message.port
  serveRpc(message.port)
  scope.postMessage({ type: "attached", threadId })
}

scope.postMessage({ type: "ready", threadId })
