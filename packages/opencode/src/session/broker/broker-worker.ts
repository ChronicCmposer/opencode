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
  type BrokerErrorCode,
  type BrokerFrame,
  type BrokerRequest,
  type BrokerWorkerInbound,
  type CallRequest,
  type ServerSpec,
} from "./protocol"
import { parseBrokerRequest, resolveRequestTimeout } from "./protocol"

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

interface Framer {
  /** Encode one JSON-RPC message into the child's stdin wire format. */
  encode(message: unknown): string
  /** Feed a chunk of the child's stdout, returning any complete messages. */
  push(chunk: Uint8Array): unknown[]
}

/** Newline-delimited JSON-RPC (MCP stdio). */
class McpFramer implements Framer {
  private buffer = ""
  private readonly decoder = new TextDecoder()

  encode(message: unknown): string {
    return `${JSON.stringify(message)}\n`
  }

  push(chunk: Uint8Array): unknown[] {
    this.buffer += this.decoder.decode(chunk, { stream: true })
    const messages: unknown[] = []
    let newline: number
    while ((newline = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, newline).trim()
      this.buffer = this.buffer.slice(newline + 1)
      if (!line) continue
      try {
        messages.push(JSON.parse(line))
      } catch {
        /* ignore malformed child output */
      }
    }
    return messages
  }
}

/** LSP-style `Content-Length` framing. */
class LspFramer implements Framer {
  private buffer = Buffer.alloc(0)

  encode(message: unknown): string {
    const json = JSON.stringify(message)
    return `Content-Length: ${Buffer.byteLength(json, "utf8")}\r\n\r\n${json}`
  }

  push(chunk: Uint8Array): unknown[] {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)])
    const messages: unknown[] = []
    for (;;) {
      const headerEnd = this.buffer.indexOf("\r\n\r\n")
      if (headerEnd < 0) break
      const header = this.buffer.slice(0, headerEnd).toString("utf8")
      const match = /Content-Length:\s*(\d+)/i.exec(header)
      if (!match) {
        this.buffer = this.buffer.slice(headerEnd + 4)
        continue
      }
      const length = Number.parseInt(match[1]!, 10)
      const bodyStart = headerEnd + 4
      if (this.buffer.length < bodyStart + length) break
      const body = this.buffer.slice(bodyStart, bodyStart + length).toString("utf8")
      this.buffer = this.buffer.slice(bodyStart + length)
      try {
        messages.push(JSON.parse(body))
      } catch {
        /* ignore malformed child output */
      }
    }
    return messages
  }
}

function framerFor(kind: ServerSpec["kind"]): Framer {
  return kind === "lsp" ? new LspFramer() : new McpFramer()
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

class BrokerFailure extends Error {
  readonly code: BrokerErrorCode
  constructor(code: BrokerErrorCode, message: string) {
    super(message)
    this.name = "BrokerFailure"
    this.code = code
  }
}

function toBrokerError(reason: unknown): BrokerError {
  if (reason instanceof BrokerFailure) return { code: reason.code, message: reason.message }
  if (reason instanceof Error) return { code: "protocol", message: reason.message }
  return { code: "protocol", message: String(reason) }
}

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

function postFrame(frame: BrokerFrame): void {
  rpc?.postMessage(frame)
}

function mergeEnv(overlay?: Readonly<Record<string, string>>): Record<string, string> {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries(process.env)) {
    if (value !== undefined) env[key] = value
  }
  for (const [key, value] of Object.entries(overlay ?? {})) env[key] = value
  return env
}

function writeChild(child: ChildHandle, text: string): void {
  const sink = child.process.stdin as Bun.FileSink | undefined
  if (!sink) return
  void sink.write(text)
  void sink.flush()
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

function dispatchChildMessage(child: ChildHandle, message: unknown): void {
  if (typeof message !== "object" || message === null) return
  const parsed = message as { id?: unknown; method?: unknown; result?: unknown; error?: unknown; params?: unknown }
  if (typeof parsed.id === "number" && child.pending.has(parsed.id)) {
    const pending = child.pending.get(parsed.id)!
    if (parsed.error !== undefined) pending.settleReject(new BrokerFailure("protocol", JSON.stringify(parsed.error)))
    else pending.settleResolve(parsed.result)
    return
  }
  if (typeof parsed.method === "string") {
    postFrame({
      kind: "notification",
      directory: child.directory,
      server: child.spec.name,
      method: parsed.method,
      ...(parsed.params === undefined ? {} : { params: parsed.params }),
    })
  }
}

function pumpChild(child: ChildHandle): void {
  const stream = child.process.stdout as ReadableStream<Uint8Array>
  const reader = stream.getReader()
  void (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        for (const message of child.framer.push(value)) dispatchChildMessage(child, message)
      }
    } catch {
      /* the child is gone; exit handling owns cleanup */
    }
  })()
}

function spawnChild(directory: string, spec: ServerSpec): ChildHandle {
  const cwd = existsDirectory(directory) ? directory : process.cwd()
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

function sendChildRequest(child: ChildHandle, method: string, params: unknown, timeoutMs: number): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    let done = false
    let timer: ReturnType<typeof setTimeout>
    const settle = (finish: () => void) => {
      if (done) return
      done = true
      clearTimeout(timer)
      child.pending.delete(childId)
      finish()
    }
    const childId = child.nextId++
    timer = setTimeout(() => settle(() => reject(new BrokerFailure("timeout", `${method} timed out after ${timeoutMs}ms`))), timeoutMs)
    child.pending.set(childId, {
      settleResolve: (value) => settle(() => resolve(value)),
      settleReject: (error) => settle(() => reject(error)),
    })
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

async function handleAcquire(request: BrokerRequest & { kind: "acquire" }): Promise<AcquireResult> {
  const state = directoryState(request.directory)
  const servers: AcquiredServer[] = []

  for (const spec of request.servers) {
    let child = state.children.get(spec.name)
    if (!child) {
      child = spawnChild(request.directory, spec)
      state.children.set(spec.name, child)
      try {
        await sendChildRequest(
          child,
          "initialize",
          { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "opencode-broker", version: "1.0.0" } },
          resolveRequestTimeout(undefined, spec.timeoutMs),
        )
      } catch (error) {
        state.children.delete(spec.name)
        stopChild(child)
        await awaitExit(child)
        throw error
      }
    }
    servers.push({ name: spec.name, pid: child.pid, pgid: child.pgid, instanceId: state.instanceId })
  }

  return { instanceId: state.instanceId, servers }
}

function dispatchCall(request: CallRequest, child: ChildHandle, timeoutMs: number): Promise<unknown> {
  let done = false
  let timer: ReturnType<typeof setTimeout>
  return new Promise<unknown>((resolve, reject) => {
    const childId = child.nextId++
    const settle = (finish: () => void) => {
      if (done) return
      done = true
      clearTimeout(timer)
      child.pending.delete(childId)
      activeCalls.delete(request.requestId)
      finish()
    }
    timer = setTimeout(
      () => settle(() => reject(new BrokerFailure("timeout", `${request.method} timed out after ${timeoutMs}ms`))),
      timeoutMs,
    )
    child.pending.set(childId, {
      settleResolve: (value) => settle(() => resolve(value)),
      settleReject: (error) => settle(() => reject(error)),
    })
    activeCalls.set(request.requestId, {
      child,
      abort: (error) => settle(() => reject(error)),
    })
    writeChild(
      child,
      child.framer.encode({ jsonrpc: "2.0", id: childId, method: request.method, ...(request.params === undefined ? {} : { params: request.params }) }),
    )
  })
}

async function handleCall(request: CallRequest): Promise<unknown> {
  Faults.crashWorker("broker:call")

  const state = directories.get(request.directory)
  if (!state) throw new BrokerFailure("unknown-directory", `no directory ${request.directory}`)
  const child = state.children.get(request.server)
  if (!child) throw new BrokerFailure("unknown-server", `no server ${request.server} in ${request.directory}`)

  const timeoutMs = resolveRequestTimeout(request.timeoutMs, child.spec.timeoutMs)
  return runSerial(state, () => {
    if (cancelled.delete(request.requestId)) {
      return Promise.reject(new BrokerFailure("cancelled", `call ${request.requestId} cancelled before dispatch`))
    }
    return dispatchCall(request, child, timeoutMs)
  })
}

function handleCancel(request: BrokerRequest & { kind: "cancel" }): void {
  const active = activeCalls.get(request.targetId)
  if (!active) {
    cancelled.add(request.targetId)
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

function onRpcMessage(event: { data: unknown }): void {
  let request: BrokerRequest
  try {
    request = parseBrokerRequest(event.data)
  } catch (error) {
    const requestId = (event.data as { requestId?: unknown } | null)?.requestId
    respondError(typeof requestId === "string" ? requestId : "unknown", toBrokerError(error))
    return
  }

  void Promise.resolve(execute(request)).then(
    (value) => respond(request.requestId, value ?? null),
    (reason) => respondError(request.requestId, toBrokerError(reason)),
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
  const message = event.data as Partial<BrokerWorkerInbound>
  if (message?.type === "attach" && message.port instanceof MessagePort) {
    rpc = message.port
    rpc.onmessage = onRpcMessage
    rpc.start?.()
    scope.postMessage({ type: "attached", threadId })
  }
}

scope.postMessage({ type: "ready", threadId })
