// Fixture worker that records passive checkpoint queries.
//
// The service supplies its `filename` option as the log path here; this worker
// ignores the file as a database and appends one line per checkpoint query. The
// test uses the line count to prove the scoped checkpoint fiber runs while the
// service is alive and stops once its scope closes.
import fs from "node:fs"
import { parentPort, threadId, workerData } from "node:worker_threads"

const transport = parentPort
if (transport === null) throw new Error("checkpoint db executor worker started without a parent port")

const logPath = String((workerData as { readonly filename?: string }).filename ?? "")
const append = (line: string) => fs.appendFileSync(logPath, `${line}\n`)

transport.postMessage({ type: "ready", threadId })
transport.on("message", (message: { readonly type: string; readonly id: number; readonly sql?: string }) => {
  if (message.type === "close") {
    transport.postMessage({ type: "closed", id: message.id })
    return
  }
  if (message.type === "query" && String(message.sql).includes("wal_checkpoint")) {
    append("checkpoint")
    transport.postMessage({ type: "result", id: message.id, value: [] })
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
