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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new BrokerProtocolError(`${field} must be a non-empty string`)
  }
  return value
}

function requireInteger(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new BrokerProtocolError(`${field} must be an integer`)
  }
  return value
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
      return { kind, requestId, directory, server, method, ...(value.params === undefined ? {} : { params: value.params }), ...(timeoutMs === undefined ? {} : { timeoutMs }) }
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

/** Total, non-throwing membership predicate; narrows to `BrokerRequest`. */
export function isBrokerRequest(value: unknown): value is BrokerRequest {
  try {
    parseBrokerRequest(value)
    return true
  } catch {
    return false
  }
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
