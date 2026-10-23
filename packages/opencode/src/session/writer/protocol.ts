// Phase 7.2 / 7.3 / 7.5 — writer RPC protocol (plan tasks 7.2, 7.3, 7.5, 7.15).
//
// The writer is a dedicated Worker isolate that owns the SOLE SQLite connection.
// Every other isolate reaches the database by sending one of these frames over the
// worker channel. The two families are deliberately distinct:
//
//   * `query` — the remote-query proxy at the `SqlClient`/`Connection` layer. It
//     carries exactly `{ method, sql, params }`, which is what the Effect SQL
//     execution path already reduces to (see `core/src/database/sqlite.bun.ts`).
//   * `admin.*` — connection-level operations (open / pragma / migrate /
//     checkpoint). These are writer-internal: no other service opens its own
//     connection, so the hub asks the writer to do it.
//
// The transaction family (`tx.*`) implements the reserved-handle protocol: a
// caller pins one transaction, runs statements against its handle, then commits or
// rolls back. A pinned transaction blocks other writers (documented head-of-line
// blocking) until it resolves or its idle timer fires.
//
// A durable `publish` returns `{ seq, committed }`; a non-durable publish is
// fire-and-forget (the caller need not await the response). Projectors cross the
// port as DATA — a registered SQL statement — never as closures (plan line 31).

import { defined } from "@/util/object"
import { createFramePrimitives } from "@/session/worker/frame"

/** The four execution shapes the Effect SQL connection reduces to. */
export type QueryMethod = "all" | "values" | "run" | "exec"

/** A durable event as it crosses the port; the aggregate id is resolved by the caller. */
export interface WriterEvent {
  readonly id: string
  readonly type: string
  readonly aggregateID: string
  readonly data: Record<string, unknown>
}

export type WriterRequest =
  | { readonly type: "admin.open"; readonly id: number; readonly filename: string }
  | { readonly type: "admin.pragma"; readonly id: number; readonly sql: string }
  | { readonly type: "admin.migrate"; readonly id: number; readonly statements: ReadonlyArray<string> }
  | { readonly type: "admin.checkpoint"; readonly id: number }
  | {
      readonly type: "query"
      readonly id: number
      readonly method: QueryMethod
      readonly sql: string
      readonly params?: ReadonlyArray<unknown>
      readonly cache?: boolean
    }
  | {
      readonly type: "register"
      readonly id: number
      readonly projectorId: string
      readonly eventType: string
      readonly sql: string
    }
  | { readonly type: "tx.begin"; readonly id: number }
  | {
      readonly type: "tx.query"
      readonly id: number
      readonly handle: string
      readonly method: QueryMethod
      readonly sql: string
      readonly params?: ReadonlyArray<unknown>
    }
  | { readonly type: "tx.commit"; readonly id: number; readonly handle: string }
  | { readonly type: "tx.rollback"; readonly id: number; readonly handle: string }
  | {
      readonly type: "publish"
      readonly id: number
      readonly event: WriterEvent
      readonly durable: boolean
      readonly ownerID?: string
      readonly strictOwner?: boolean
      readonly commitId?: string
    }
  | { readonly type: "claim"; readonly id: number; readonly aggregateID: string; readonly ownerID: string }
  | { readonly type: "steal"; readonly id: number; readonly aggregateID: string; readonly ownerID: string }
  | { readonly type: "remove"; readonly id: number; readonly aggregateID: string }
  // Non-SQL mutation family: the file-KV `Storage` (plan task 7.13). These cross
  // the port as plain data and serialize through the same writer queue as SQL, so
  // file-KV mutations never bypass the single-writer boundary.
  | { readonly type: "admin.openStorage"; readonly id: number; readonly dir: string }
  | { readonly type: "storage.read"; readonly id: number; readonly key: ReadonlyArray<string> }
  | {
      readonly type: "storage.write"
      readonly id: number
      readonly key: ReadonlyArray<string>
      readonly content: unknown
    }
  | { readonly type: "storage.remove"; readonly id: number; readonly key: ReadonlyArray<string> }
  | { readonly type: "storage.list"; readonly id: number; readonly prefix: ReadonlyArray<string> }
  | {
      readonly type: "replay"
      readonly id: number
      readonly event: WriterEvent
      readonly seq: number
      readonly ownerID?: string
      readonly strictOwner?: boolean
    }

export interface WriterReady {
  readonly type: "ready"
  readonly generation: number
}

export interface WriterResult {
  readonly type: "result"
  readonly id: number
  readonly value: unknown
}

/** A serialized writer error; `tag` is the typed failure channel. */
export interface WriterErrorInfo {
  readonly tag: string
  readonly message: string
  /**
   * The native SQLite code (`SQLITE_BUSY`, `SQLITE_BUSY_SNAPSHOT`, …), when the
   * underlying driver error carried one. Carrying it across the port lets the
   * receiving side classify a lock as a `LockTimeoutError` instead of collapsing
   * to `UnknownError`.
   */
  readonly code?: string | number
}

/**
 * The native code carried by a driver error, if any. Reads `code` (bun:sqlite /
 * node:sqlite) or `sqliteCode` (the injected `CommitFault`), so a writer failure
 * keeps the code the receiver needs to classify it.
 */
export function errorCode(error: unknown): string | number | undefined {
  if (typeof error !== "object" || error === null) return undefined
  const record = error as { code?: unknown; sqliteCode?: unknown }
  if (typeof record.code === "string" || typeof record.code === "number") return record.code
  if (typeof record.sqliteCode === "string" || typeof record.sqliteCode === "number") return record.sqliteCode
  return undefined
}

export interface WriterFailure {
  readonly type: "failure"
  readonly id: number
  readonly error: WriterErrorInfo
}

export type WriterResponse = WriterReady | WriterResult | WriterFailure

/** Raised when a frame does not match the protocol; a caller bug, so fail loud. */
export class ProtocolError extends Error {
  readonly tag = "PROTOCOL"
  constructor(message: string) {
    super(message)
    this.name = "ProtocolError"
  }
}

const QUERY_METHODS: ReadonlySet<string> = new Set<QueryMethod>(["all", "values", "run", "exec"])

/**
 * True when `error` is a Node-style ENOENT (missing file or directory). Declared in
 * this dependency-free module so both the writer's FS backend and the direct-FS
 * `Storage` service can share it without either pulling in the other's stack.
 * Effect's FS adapter additionally recognizes `reason._tag === "NotFound"` itself.
 */
export function isEnoent(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { code: unknown }).code === "ENOENT"
}

/**
 * The shared frame primitives bound to this module's error type. The "plain
 * object / required string / required finite number / optional" parsers live in
 * `session/worker/frame.ts` so the writer, broker and events protocols share one
 * implementation; binding `createFramePrimitives` keeps `ProtocolError` as the
 * loud failure channel this boundary already failed with.
 */
const { isRecord, requireString, requireNumber, optionalString, optionalBoolean } = createFramePrimitives(
  (message) => {
    throw new ProtocolError(message)
  },
)

function requireStringArray(record: Record<string, unknown>, field: string): string[] {
  const value = record[field]
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    throw new ProtocolError(`field "${field}" must be a string array (got ${JSON.stringify(value)})`)
  }
  return value as string[]
}

function optionalParams(record: Record<string, unknown>): ReadonlyArray<unknown> | undefined {
  const value = record["params"]
  if (value === undefined) return undefined
  if (!Array.isArray(value)) throw new ProtocolError(`field "params" must be an array (got ${JSON.stringify(value)})`)
  return value
}

function parseMethod(record: Record<string, unknown>): QueryMethod {
  const method = record["method"]
  if (typeof method !== "string" || !QUERY_METHODS.has(method)) {
    throw new ProtocolError(`field "method" must be one of all, values, run, exec (got ${JSON.stringify(method)})`)
  }
  return method as QueryMethod
}

function parseEvent(record: Record<string, unknown>): WriterEvent {
  const event = record["event"]
  if (!isRecord(event)) throw new ProtocolError(`field "event" must be an object (got ${JSON.stringify(event)})`)
  const data = event["data"]
  if (!isRecord(data)) throw new ProtocolError(`field "event.data" must be an object (got ${JSON.stringify(data)})`)
  return {
    id: requireString(event, "id"),
    type: requireString(event, "type"),
    aggregateID: requireString(event, "aggregateID"),
    data: data as Record<string, unknown>,
  }
}

function optionalCode(record: Record<string, unknown>, field: string): string | number | undefined {
  const value = record[field]
  if (value === undefined) return undefined
  if (typeof value !== "string" && typeof value !== "number") {
    throw new ProtocolError(`field "${field}" must be a string or number (got ${JSON.stringify(value)})`)
  }
  return value
}

/**
 * Parse an untrusted worker message into a trusted request, failing loudly. Once a
 * frame has passed this boundary the worker trusts it completely.
 */
export function parseWriterRequest(input: unknown): WriterRequest {
  if (!isRecord(input)) throw new ProtocolError(`request must be an object (got ${JSON.stringify(input)})`)
  const type = input["type"]
  if (typeof type !== "string") throw new ProtocolError(`field "type" must be a string (got ${JSON.stringify(type)})`)
  const id = requireNumber(input, "id")

  switch (type) {
    case "admin.open":
      return { type, id, filename: requireString(input, "filename") }
    case "admin.pragma":
      return { type, id, sql: requireString(input, "sql") }
    case "admin.migrate": {
      const statements = input["statements"]
      if (!Array.isArray(statements) || !statements.every((s) => typeof s === "string")) {
        throw new ProtocolError(`field "statements" must be a string array (got ${JSON.stringify(statements)})`)
      }
      return { type, id, statements: statements as string[] }
    }
    case "admin.checkpoint":
      return { type, id }
    case "query": {
      const params = optionalParams(input)
      const cache = optionalBoolean(input, "cache")
      return {
        type,
        id,
        method: parseMethod(input),
        sql: requireString(input, "sql"),
        ...defined({ params, cache }),
      }
    }
    case "register":
      return {
        type,
        id,
        projectorId: requireString(input, "projectorId"),
        eventType: requireString(input, "eventType"),
        sql: requireString(input, "sql"),
      }
    case "tx.begin":
      return { type, id }
    case "tx.query": {
      const params = optionalParams(input)
      return {
        type,
        id,
        handle: requireString(input, "handle"),
        method: parseMethod(input),
        sql: requireString(input, "sql"),
        ...defined({ params }),
      }
    }
    case "tx.commit":
      return { type, id, handle: requireString(input, "handle") }
    case "tx.rollback":
      return { type, id, handle: requireString(input, "handle") }
    case "publish": {
      const ownerID = optionalString(input, "ownerID")
      const strictOwner = optionalBoolean(input, "strictOwner")
      const commitId = optionalString(input, "commitId")
      return {
        type,
        id,
        event: parseEvent(input),
        durable: optionalBoolean(input, "durable") ?? false,
        ...defined({ ownerID, strictOwner, commitId }),
      }
    }
    case "claim":
      return { type, id, aggregateID: requireString(input, "aggregateID"), ownerID: requireString(input, "ownerID") }
    case "steal":
      return { type, id, aggregateID: requireString(input, "aggregateID"), ownerID: requireString(input, "ownerID") }
    case "remove":
      return { type, id, aggregateID: requireString(input, "aggregateID") }
    case "admin.openStorage":
      return { type, id, dir: requireString(input, "dir") }
    case "storage.read":
      return { type, id, key: requireStringArray(input, "key") }
    case "storage.write":
      return { type, id, key: requireStringArray(input, "key"), content: input["content"] }
    case "storage.remove":
      return { type, id, key: requireStringArray(input, "key") }
    case "storage.list":
      return { type, id, prefix: requireStringArray(input, "prefix") }
    case "replay": {
      const ownerID = optionalString(input, "ownerID")
      const strictOwner = optionalBoolean(input, "strictOwner")
      return {
        type,
        id,
        event: parseEvent(input),
        seq: requireNumber(input, "seq"),
        ...defined({ ownerID, strictOwner }),
      }
    }
    default:
      throw new ProtocolError(`unknown request type "${type}"`)
  }
}

/**
 * Parse an untrusted worker response. The client is the boundary here: a message
 * that does not match the protocol is a bug in the writer isolate and must fail
 * loud rather than be trusted by field.
 */
export function parseWriterResponse(input: unknown): WriterResponse {
  if (!isRecord(input)) throw new ProtocolError(`response must be an object (got ${JSON.stringify(input)})`)
  const type = input["type"]
  switch (type) {
    case "ready":
      return { type: "ready", generation: requireNumber(input, "generation") }
    case "result":
      return { type: "result", id: requireNumber(input, "id"), value: input["value"] }
    case "failure": {
      const error = input["error"]
      if (!isRecord(error)) throw new ProtocolError(`field "error" must be an object (got ${JSON.stringify(error)})`)
      const code = optionalCode(error, "code")
      return {
        type: "failure",
        id: requireNumber(input, "id"),
        error: { tag: requireString(error, "tag"), message: requireString(error, "message"), ...defined({ code }) },
      }
    }
    default:
      throw new ProtocolError(`unknown response type "${String(type)}"`)
  }
}

export * as WriterProtocol from "./protocol"
