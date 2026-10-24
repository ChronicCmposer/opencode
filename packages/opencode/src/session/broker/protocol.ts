// Phase 10 — broker wire protocol (plan tasks 10.2/10.3).
//
// The MCP/LSP broker is a dedicated Worker (not in K, not headroom). The hub
// and every session worker talk to it over a transferred `MessagePort` with
// request-id-routed frames, so a slow MCP call never lands on the hub loop.
//
// This module is the *pure* boundary shared by the worker entry point
// (`broker-worker.ts`) and the hub client (`broker.ts`):
//
//   - the frame/request shapes (types only),
//   - `parseBrokerRequest`, which turns an untrusted `unknown` into a trusted
//     `BrokerRequest` (parse, don't validate: the worker never re-checks a
//     parsed field), and
//   - the timeout resolution used to enforce `mcp_timeout`.
//
// It has no side effects and may be imported by both isolates.

import { createFramePrimitives } from "@/session/worker/frame"

/** Default `mcp_timeout` in ms, mirroring `src/mcp/index.ts` `DEFAULT_TIMEOUT`. */
export const DEFAULT_MCP_TIMEOUT_MS = 30_000

export type ServerKind = "mcp" | "lsp"

/** How the broker should launch and talk to one MCP or LSP server child. */
export interface ServerSpec {
  readonly name: string
  readonly kind: ServerKind
  readonly command: ReadonlyArray<string>
  readonly env?: Readonly<Record<string, string>>
  /** Per-server request timeout in ms; falls back to the broker default. */
  readonly timeoutMs?: number
}

export interface AcquireRequest {
  readonly kind: "acquire"
  readonly requestId: string
  readonly directory: string
  readonly servers: ReadonlyArray<ServerSpec>
}

export interface CallRequest {
  readonly kind: "call"
  readonly requestId: string
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
  readonly timeoutMs?: number
}

export interface CancelRequest {
  readonly kind: "cancel"
  readonly requestId: string
  /** The `requestId` of the in-flight call to abandon. */
  readonly targetId: string
}

export interface ReleaseRequest {
  readonly kind: "release"
  readonly requestId: string
  readonly directory: string
}

export interface OAuthRequest {
  readonly kind: "oauth"
  readonly requestId: string
  readonly directory: string
}

export interface PingRequest {
  readonly kind: "ping"
  readonly requestId: string
}

export interface TeardownRequest {
  readonly kind: "teardown"
  readonly requestId: string
}

export type BrokerRequest =
  | AcquireRequest
  | CallRequest
  | CancelRequest
  | ReleaseRequest
  | OAuthRequest
  | PingRequest
  | TeardownRequest

export type BrokerErrorCode =
  | "timeout"
  | "cancelled"
  | "crashed"
  | "invalid-request"
  | "unknown-directory"
  | "unknown-server"
  | "spawn-failed"
  | "handshake-failed"
  | "child-exited"
  | "protocol"

export interface BrokerError {
  readonly code: BrokerErrorCode
  readonly message: string
}

/** Raised when a request frame cannot be parsed at the boundary. */
export class BrokerProtocolError extends Error {
  readonly detail: string
  constructor(detail: string) {
    super(`invalid broker request: ${detail}`)
    this.name = "BrokerProtocolError"
    this.detail = detail
  }
}

/**
 * A broker-level failure carrying the wire error taxonomy. Shared by the pure
 * protocol boundary (`resolveRequestTimeout`) and the broker worker, so both
 * speak one failure type; the worker's `toBrokerError` maps it onto a
 * `BrokerError` frame.
 */
export class BrokerFailure extends Error {
  readonly code: BrokerErrorCode
  constructor(code: BrokerErrorCode, message: string) {
    super(message)
    this.name = "BrokerFailure"
    this.code = code
  }
}

export interface ResponseValueFrame {
  readonly kind: "response"
  readonly requestId: string
  readonly ok: true
  readonly value: unknown
}

export interface ResponseErrorFrame {
  readonly kind: "response"
  readonly requestId: string
  readonly ok: false
  readonly error: BrokerError
}

export type ResponseFrame = ResponseValueFrame | ResponseErrorFrame

/** An MCP/LSP notification fanned out from a child to every subscriber. */
export interface NotificationFrame {
  readonly kind: "notification"
  readonly directory: string
  readonly server: string
  readonly method: string
  readonly params?: unknown
}

/** A newly spawned child's identity, mirrored into the hub's PGID registry. */
export interface ChildFrame {
  readonly kind: "child"
  readonly directory: string
  readonly server: string
  readonly pid: number
  readonly pgid: number
}

/** A child that has exited (or been reaped on teardown/release). */
export interface ChildExitFrame {
  readonly kind: "child-exit"
  readonly directory: string
  readonly server: string
  readonly pid: number
  readonly pgid: number
  readonly code: number | null
}

export type BrokerFrame = ResponseFrame | NotificationFrame | ChildFrame | ChildExitFrame

// Handshake frames travel over the default worker channel; RPC then moves to a
// transferred `MessagePort` (the "port-pairing handshake").

export interface WorkerReadyFrame {
  readonly type: "ready"
  readonly threadId: number
}

export interface WorkerAttachFrame {
  readonly type: "attach"
  readonly port: MessagePort
}

export interface WorkerAttachedFrame {
  readonly type: "attached"
  readonly threadId: number
}

export type BrokerWorkerOutbound = WorkerReadyFrame | WorkerAttachedFrame
export type BrokerWorkerInbound = WorkerAttachFrame

export interface AcquiredServer {
  readonly name: string
  readonly pid: number
  readonly pgid: number
  /** Stable per-directory instance id, shared across all session workers. */
  readonly instanceId: string
}

export interface AcquireResult {
  readonly instanceId: string
  readonly servers: ReadonlyArray<AcquiredServer>
}

export interface OAuthResult {
  readonly callbackUrl: string
  readonly instanceId: string
  readonly owner: "broker"
}

export interface PingResult {
  readonly threadId: number
  readonly directories: number
}

const SERVER_KINDS: ReadonlyArray<ServerKind> = ["mcp", "lsp"]

/**
 * The shared frame primitives bound to the broker's own error type: one
 * implementation of the "plain object / required field" parsers lives in
 * `session/worker/frame.ts`, so the broker, writer and events protocols cannot
 * drift. Binding `createFramePrimitives` keeps `BrokerProtocolError` as the loud
 * failure channel this module's callers already expect.
 */
const frame = createFramePrimitives((message) => {
  throw new BrokerProtocolError(message)
})

/** True for a plain object; `null` and arrays are excluded (shared parser). */
const { isRecord } = frame

/**
 * Parse one required string field, delegating the type check and loud message to
 * the shared parser. The broker's parsers take the field VALUE plus its path, so
 * the value is presented to the shared record-based parser under its own path
 * (`{ [field]: value }`): the lookup and the message then both use the path.
 *
 * On top of the shared check the broker rejects an empty string: every broker
 * string field (`kind`, `requestId`, `directory`, `server`, `method`, `name`,
 * `instanceId`) is meaningless when blank.
 */
function requireString(value: unknown, field: string): string {
  const parsed = frame.requireString({ [field]: value }, field)
  if (parsed.length === 0) throw new BrokerProtocolError(`${field} must be a non-empty string`)
  return parsed
}

/** Parse one required integer field through the shared parser (value + path). */
function requireInteger(value: unknown, field: string): number {
  return frame.requireInteger({ [field]: value }, field)
}

/** Cap for the broker's queued-cancellation registry. */
export const DEFAULT_CANCELLED_CAP = 1024

/**
 * Insert `id` into `set`, evicting the oldest entry first once `cap` is reached.
 * A cancellation that never matches a dispatched call must not grow the registry
 * without bound (plan A14). `Set` iterates in insertion order, so "oldest" is the
 * first value.
 */
export function rememberBounded<T>(set: Set<T>, id: T, cap: number = DEFAULT_CANCELLED_CAP): void {
  if (!Number.isInteger(cap) || cap <= 0) {
    throw new BrokerProtocolError(`cap must be a positive integer (got ${String(cap)})`)
  }
  if (set.has(id)) return
  if (set.size >= cap) {
    // Read the iterator's `done` flag rather than comparing the value to
    // `undefined`: a set may legitimately store `undefined`, and mistaking that
    // member for "no oldest entry" would skip the eviction and exceed the cap.
    const oldest = set.values().next()
    if (!oldest.done) set.delete(oldest.value)
  }
  set.add(id)
}

function requirePositiveInteger(value: unknown, field: string): number {
  const parsed = requireInteger(value, field)
  if (parsed <= 0) throw new BrokerProtocolError(`${field} must be a positive integer`)
  return parsed
}

function isServerKind(value: string): value is ServerKind {
  return (SERVER_KINDS as ReadonlyArray<string>).includes(value)
}

function parseServerKind(value: unknown, field: string): ServerKind {
  const kind = requireString(value, field)
  if (!isServerKind(kind)) {
    throw new BrokerProtocolError(`${field} must be "mcp" or "lsp", got "${kind}"`)
  }
  return kind
}

/** Build a genuinely `string[]`-typed command, not a re-cast `unknown[]`. */
function parseCommand(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new BrokerProtocolError(`${field} must be a non-empty string array`)
  }
  const command: string[] = []
  for (const part of value) {
    if (typeof part !== "string") {
      throw new BrokerProtocolError(`${field} must be a non-empty string array`)
    }
    command.push(part)
  }
  return command
}

function parseServer(value: unknown, index: number): ServerSpec {
  if (!isRecord(value)) throw new BrokerProtocolError(`servers[${index}] must be an object`)
  const name = requireString(value.name, `servers[${index}].name`)
  const kind = parseServerKind(value.kind, `servers[${index}].kind`)
  const command = parseCommand(value.command, `servers[${index}].command`)
  const env = parseEnv(value.env, `servers[${index}].env`)
  const timeoutMs = parseOptionalTimeout(value.timeoutMs, `servers[${index}].timeoutMs`)
  return { name, kind, command, ...(env ? { env } : {}), ...(timeoutMs === undefined ? {} : { timeoutMs }) }
}

function parseEnv(value: unknown, field: string): Readonly<Record<string, string>> | undefined {
  if (value === undefined) return undefined
  if (!isRecord(value)) throw new BrokerProtocolError(`${field} must be an object`)
  const entries: Array<[string, string]> = []
  for (const [key, item] of Object.entries(value)) {
    if (typeof item !== "string") throw new BrokerProtocolError(`${field}.${key} must be a string`)
    entries.push([key, item])
  }
  return Object.fromEntries(entries)
}

function parseOptionalTimeout(value: unknown, field: string): number | undefined {
  if (value === undefined) return undefined
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new BrokerProtocolError(`${field} must be a positive number`)
  }
  return value
}

/**
 * Parse untrusted input into a trusted `BrokerRequest`. Throws
 * `BrokerProtocolError` with the offending field so a malformed frame halts
 * immediately instead of entering the broker's state machine half-formed.
 */
export function parseBrokerRequest(value: unknown): BrokerRequest {
  if (!isRecord(value)) throw new BrokerProtocolError("request must be an object")
  const kind = requireString(value.kind, "kind")
  const requestId = requireString(value.requestId, "requestId")

  switch (kind) {
    case "acquire": {
      const directory = requireString(value.directory, "directory")
      if (!Array.isArray(value.servers) || value.servers.length === 0) {
        throw new BrokerProtocolError("servers must be a non-empty array")
      }
      return { kind, requestId, directory, servers: value.servers.map(parseServer) }
    }
    case "call": {
      const directory = requireString(value.directory, "directory")
      const server = requireString(value.server, "server")
      const method = requireString(value.method, "method")
      const timeoutMs = parseOptionalTimeout(value.timeoutMs, "timeoutMs")
      return {
        kind,
        requestId,
        directory,
        server,
        method,
        ...(value.params === undefined ? {} : { params: value.params }),
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
      }
    }
    case "cancel":
      return { kind, requestId, targetId: requireString(value.targetId, "targetId") }
    case "release":
      return { kind, requestId, directory: requireString(value.directory, "directory") }
    case "oauth":
      return { kind, requestId, directory: requireString(value.directory, "directory") }
    case "ping":
      return { kind, requestId }
    case "teardown":
      return { kind, requestId }
    default:
      throw new BrokerProtocolError(`unknown kind "${kind}"`)
  }
}

const BROKER_ERROR_CODES: ReadonlyArray<BrokerErrorCode> = [
  "timeout",
  "cancelled",
  "crashed",
  "invalid-request",
  "unknown-directory",
  "unknown-server",
  "spawn-failed",
  "handshake-failed",
  "child-exited",
  "protocol",
]

function isBrokerErrorCode(value: unknown): value is BrokerErrorCode {
  return typeof value === "string" && (BROKER_ERROR_CODES as ReadonlyArray<string>).includes(value)
}

/**
 * Map any thrown failure onto the wire error taxonomy. This is the single
 * mapping shared by both broker boundaries (the hub port server and the broker
 * worker's RPC loop): a `BrokerFailure` or any error already carrying a valid
 * broker code keeps that code, an unparseable request is `invalid-request`, and
 * anything else is `protocol`.
 */
export function toBrokerError(reason: unknown): BrokerError {
  if (reason instanceof BrokerFailure) return { code: reason.code, message: reason.message }
  if (reason instanceof BrokerProtocolError) return { code: "invalid-request", message: reason.message }
  if (isRecord(reason) && isBrokerErrorCode(reason["code"])) {
    return { code: reason["code"], message: reason instanceof Error ? reason.message : String(reason) }
  }
  return { code: "protocol", message: reason instanceof Error ? reason.message : String(reason) }
}

function parseBrokerError(value: unknown): BrokerError {
  if (!isRecord(value)) throw new BrokerProtocolError("error must be an object")
  const code = requireString(value.code, "error.code")
  if (!(BROKER_ERROR_CODES as ReadonlyArray<string>).includes(code)) {
    throw new BrokerProtocolError(`unknown broker error code "${code}"`)
  }
  return { code: code as BrokerErrorCode, message: requireString(value.message, "error.message") }
}

function parseExitCode(value: unknown): number | null {
  return value === null ? null : requireInteger(value, "code")
}

/**
 * Parse an untrusted broker→hub frame arriving on the RPC port. The port is the
 * trust boundary: a malformed frame is a broker bug and fails loud, so the hub's
 * pending/mirror state never accepts a half-formed frame.
 */
export function parseBrokerFrame(value: unknown): BrokerFrame {
  if (!isRecord(value)) throw new BrokerProtocolError("frame must be an object")
  const kind = requireString(value.kind, "kind")
  switch (kind) {
    case "response": {
      const requestId = requireString(value.requestId, "requestId")
      if (value.ok === true) return { kind, requestId, ok: true, value: value.value ?? null }
      if (value.ok !== false) throw new BrokerProtocolError('response field "ok" must be a boolean')
      return { kind, requestId, ok: false, error: parseBrokerError(value.error) }
    }
    case "notification":
      return {
        kind,
        directory: requireString(value.directory, "directory"),
        server: requireString(value.server, "server"),
        method: requireString(value.method, "method"),
        ...(value.params === undefined ? {} : { params: value.params }),
      }
    case "child":
      return {
        kind,
        directory: requireString(value.directory, "directory"),
        server: requireString(value.server, "server"),
        pid: requirePositiveInteger(value.pid, "pid"),
        pgid: requirePositiveInteger(value.pgid, "pgid"),
      }
    case "child-exit":
      return {
        kind,
        directory: requireString(value.directory, "directory"),
        server: requireString(value.server, "server"),
        pid: requirePositiveInteger(value.pid, "pid"),
        pgid: requirePositiveInteger(value.pgid, "pgid"),
        code: parseExitCode(value.code),
      }
    default:
      throw new BrokerProtocolError(`unknown broker frame kind "${kind}"`)
  }
}

/**
 * Parse an untrusted broker-worker→hub handshake frame (`ready` / `attached`),
 * so the hub never branches on an unchecked `type`/`threadId` pair.
 */
export function parseWorkerHandshake(value: unknown): BrokerWorkerOutbound {
  if (!isRecord(value)) throw new BrokerProtocolError("handshake must be an object")
  const type = requireString(value.type, "type")
  switch (type) {
    case "ready":
      return { type, threadId: requireInteger(value.threadId, "threadId") }
    case "attached":
      return { type, threadId: requireInteger(value.threadId, "threadId") }
    default:
      throw new BrokerProtocolError(`unknown handshake type "${type}"`)
  }
}

/**
 * Parse the hub→broker-worker `attach` handshake. The transferred `MessagePort`
 * is the only payload and must be a real port.
 */
export function parseWorkerAttach(value: unknown): BrokerWorkerInbound {
  if (!isRecord(value)) throw new BrokerProtocolError("attach frame must be an object")
  if (value.type !== "attach") throw new BrokerProtocolError('attach frame must have type "attach"')
  if (!(value.port instanceof MessagePort)) throw new BrokerProtocolError("attach frame must carry a MessagePort")
  return { type: "attach", port: value.port }
}

function parseAcquiredServer(value: unknown, index: number): AcquiredServer {
  if (!isRecord(value)) throw new BrokerProtocolError(`servers[${index}] must be an object`)
  return {
    name: requireString(value.name, `servers[${index}].name`),
    pid: requirePositiveInteger(value.pid, `servers[${index}].pid`),
    pgid: requirePositiveInteger(value.pgid, `servers[${index}].pgid`),
    instanceId: requireString(value.instanceId, `servers[${index}].instanceId`),
  }
}

/** Parse an untrusted `acquire` response value into a trusted `AcquireResult`. */
export function parseAcquireResult(value: unknown): AcquireResult {
  if (!isRecord(value)) throw new BrokerProtocolError("acquire result must be an object")
  const instanceId = requireString(value.instanceId, "instanceId")
  if (!Array.isArray(value.servers)) throw new BrokerProtocolError("acquire result servers must be an array")
  return { instanceId, servers: value.servers.map(parseAcquiredServer) }
}

/** Parse an untrusted `oauth` response value into a trusted `OAuthResult`. */
export function parseOAuthResult(value: unknown): OAuthResult {
  if (!isRecord(value)) throw new BrokerProtocolError("oauth result must be an object")
  const callbackUrl = requireString(value.callbackUrl, "callbackUrl")
  const instanceId = requireString(value.instanceId, "instanceId")
  if (value.owner !== "broker") throw new BrokerProtocolError('oauth result owner must be "broker"')
  return { callbackUrl, instanceId, owner: "broker" }
}

/** Parse an untrusted `ping` response value into a trusted `PingResult`. */
export function parsePingResult(value: unknown): PingResult {
  if (!isRecord(value)) throw new BrokerProtocolError("ping result must be an object")
  return {
    threadId: requireInteger(value.threadId, "threadId"),
    directories: requireInteger(value.directories, "directories"),
  }
}

/**
 * Resolve the effective timeout for a request: explicit request override, then
 * the server spec, then `DEFAULT_MCP_TIMEOUT_MS`. A non-positive value is a
 * configuration bug and throws a `BrokerFailure` (not a parse error), because
 * the value reached internal logic already parsed.
 */
export function resolveRequestTimeout(requestTimeout?: number, specTimeout?: number): number {
  const chosen = requestTimeout ?? specTimeout ?? DEFAULT_MCP_TIMEOUT_MS
  if (typeof chosen !== "number" || !Number.isFinite(chosen) || chosen <= 0) {
    throw new BrokerFailure("invalid-request", `timeout must be a positive number, got ${String(chosen)}`)
  }
  return chosen
}
