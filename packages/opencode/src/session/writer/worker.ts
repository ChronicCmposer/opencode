// Phase 7 — writer Worker entry (plan tasks 7.2, 7.3, 7.5, 7.13, 7.16).
//
// This is the isolate that owns the SOLE SQLite connection. It is loaded only as a
// Bun Worker entry point (top-level side effects), so it has no self-reexport;
// import its types with `import type` from the hub side.
//
//   on load                                   -> out: { type: "ready", generation }
//   in : WriterRequest (see ./protocol)       -> out: { type: "result", id, value }
//                                             -> out: { type: "failure", id, error }
//
// The connection is created by the `admin.open` frame (no service opens its own
// connection). Every other frame is answered by the serialized `WriterCore`.

import { workerData } from "node:worker_threads"
import { createBunSqliteConnection, type WriterConnection } from "./connection"
import { createWriterCore, writerErrorInfo, type WriterCore } from "./core"
import { createFileStorage } from "./storage"
import { parseWriterRequest, type WriterRequest, type WriterResponse } from "./protocol"

/** Busy timeout is deliberately short: a sync driver with a long timeout stalls the writer thread. */
const BUSY_TIMEOUT_MS = 250

interface WorkerScope {
  postMessage(message: WriterResponse): void
  onmessage: ((event: { data: unknown }) => void) | null
}

const scope = globalThis as unknown as WorkerScope

function readGeneration(): number {
  const value = (workerData as { generation?: unknown } | undefined)?.generation
  // Absent generation is a direct (non-client) spawn; a present-but-invalid one is
  // a client bug, so fail loud rather than silently falling back to generation 1.
  if (value === undefined) return 1
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new Error(`invalid writer generation ${JSON.stringify(value)}; expected an integer >= 1`)
  }
  return value
}

const generation = readGeneration()

let connection: WriterConnection | undefined
let core: WriterCore | undefined

function requireCore(): WriterCore {
  if (core === undefined) throw new Error("writer connection is not open; send admin.open first")
  return core
}

async function openConnection(filename: string): Promise<void> {
  if (connection !== undefined) throw new Error("writer connection is already open")
  // Build the whole graph before publishing any of it, so a failure partway leaves
  // the isolate unopened (and the native handle closed) rather than half-open.
  const nextConnection = createBunSqliteConnection({
    filename,
    pragmas: ["PRAGMA journal_mode = WAL", `PRAGMA busy_timeout = ${BUSY_TIMEOUT_MS}`],
  })
  let nextCore: WriterCore
  try {
    nextCore = createWriterCore({ connection: nextConnection })
  } catch (error) {
    nextConnection.close()
    throw error
  }
  connection = nextConnection
  core = nextCore
}

async function handle(request: WriterRequest): Promise<unknown> {
  switch (request.type) {
    case "admin.open":
      await openConnection(request.filename)
      return { opened: true, filename: request.filename }
    case "admin.pragma":
      await requireCore().pragma(request.sql)
      return null
    case "admin.migrate":
      await requireCore().migrate(request.statements)
      return null
    case "admin.checkpoint":
      await requireCore().checkpoint()
      return null
    case "admin.openStorage":
      requireCore().setStorage(createFileStorage(request.dir))
      return null
    case "storage.read":
      return requireCore().storageRead(request.key)
    case "storage.write":
      await requireCore().storageWrite(request.key, request.content)
      return null
    case "storage.remove":
      await requireCore().storageRemove(request.key)
      return null
    case "storage.list":
      return requireCore().storageList(request.prefix)
    case "query":
      return requireCore().query(request.method, request.sql, request.params ?? [], { cache: request.cache })
    case "register":
      await requireCore().registerProjector(request.projectorId, request.eventType, request.sql)
      return null
    case "tx.begin":
      return requireCore().beginTransaction()
    case "tx.query":
      return requireCore().txQuery(request.handle, request.method, request.sql, request.params ?? [])
    case "tx.commit":
      await requireCore().commitTransaction(request.handle)
      return null
    case "tx.rollback":
      await requireCore().rollbackTransaction(request.handle)
      return null
    case "publish":
      return requireCore().publish({
        event: request.event,
        durable: request.durable,
        ...(request.ownerID === undefined ? {} : { ownerID: request.ownerID }),
        ...(request.strictOwner === undefined ? {} : { strictOwner: request.strictOwner }),
        ...(request.commitId === undefined ? {} : { commitId: request.commitId }),
      })
    case "claim":
      await requireCore().claim(request.aggregateID, request.ownerID)
      return null
    case "steal":
      await requireCore().steal(request.aggregateID, request.ownerID)
      return null
    case "remove":
      await requireCore().remove(request.aggregateID)
      return null
    case "replay":
      return requireCore().replay({
        event: request.event,
        seq: request.seq,
        ...(request.ownerID === undefined ? {} : { ownerID: request.ownerID }),
        ...(request.strictOwner === undefined ? {} : { strictOwner: request.strictOwner }),
      })
    default: {
      // Exhaustiveness guard: a new frame must be handled here or this fails to compile.
      const _exhaustive: never = request
      throw new Error(`unhandled writer request: ${JSON.stringify(_exhaustive)}`)
    }
  }
}

scope.onmessage = (event) => {
  let request: WriterRequest
  try {
    request = parseWriterRequest(event.data)
  } catch (error) {
    scope.postMessage({ type: "failure", id: -1, error: writerErrorInfo(error) })
    return
  }

  void handle(request).then(
    (value) => scope.postMessage({ type: "result", id: request.id, value: value ?? null }),
    (error) => scope.postMessage({ type: "failure", id: request.id, error: writerErrorInfo(error) }),
  )
}

scope.postMessage({ type: "ready", generation })
