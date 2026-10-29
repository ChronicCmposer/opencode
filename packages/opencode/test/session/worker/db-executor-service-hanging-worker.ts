// Fixture worker for the DbExecutor service tests.
//
// It answers the boot handshake and every request except `SLOW`, which it
// deliberately never answers. That lets a test interrupt (or time out) an
// in-flight request while the isolate is still healthy, and lets the orderly
// close handshake prove teardown still completes.
//
// The service forwards its `filename` as worker data. This fixture never opens
// a database, so an absolute `filename` is used as a readiness marker: writing
// it when `SLOW` arrives publishes "the request is in flight" to the test,
// which can then interrupt without a fixed wall-clock sleep.
import fs from "node:fs"
import path from "node:path"
import { parentPort, threadId, workerData } from "node:worker_threads"

const transport = parentPort
if (transport === null) throw new Error("hanging db executor worker started without a parent port")

const markerPath = (workerData as { readonly filename?: string }).filename

transport.postMessage({ type: "ready", threadId })
transport.on("message", (message: { readonly type: string; readonly id: number; readonly sql?: string }) => {
  if (message.type === "close") {
    transport.postMessage({ type: "closed", id: message.id })
    return
  }
  if (message.type === "query" && message.sql === "SLOW") {
    if (markerPath !== undefined && path.isAbsolute(markerPath)) fs.writeFileSync(markerPath, "received")
    return
  }
  if (message.type === "query") {
    transport.postMessage({ type: "result", id: message.id, value: [{ n: 1 }] })
    return
  }
  if (message.type === "run") {
    transport.postMessage({ type: "result", id: message.id, value: { changes: 0, lastInsertRowid: 0 } })
    return
  }
  if (message.type === "transaction") {
    transport.postMessage({ type: "result", id: message.id, value: [] })
  }
})
