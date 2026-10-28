// Bootstrap disposal hardening.
//
// `bootstrapSessionWorker` opens the per-worker WAL database before it returns
// the handle. A throw between that open and the returned handle used to rely on
// isolate teardown: `closeResources` (and with it the SQLite handle/WAL close)
// was only reachable from the returned `dispose()` or the hub `shutdown` frame,
// so a failed mid-bootstrap leaked the handle until the process exited.
//
// This test injects a real throw in that window through the existing
// `installExecutor` seam (which runs after the DB, writer lease, clients and
// worker scopes are built) and proves the bootstrap now runs the same orderly
// teardown and rethrows the original error.

import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Flag } from "@opencode-ai/core/flag/flag"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity } from "@/session/worker/db"
import {
  bootstrapSessionWorker,
  type SessionWorkerInit,
  type WorkerWriterClient,
} from "@/session/worker/session-worker"
import { workerSnapshot } from "../../lib/snapshot"

// A full-port bootstrap opens the worker's per-worker WAL connection at
// `Database.path()`. Point that at a migrated file database for this file so the
// boot exercises the real schema gate instead of the suite's `:memory:` default.
const previousDatabasePath = Flag.OPENCODE_DB
let databaseDirectory: string | undefined
let databaseFilename: string

beforeAll(() => {
  databaseDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-bootstrap-dispose-"))
  databaseFilename = path.join(databaseDirectory, "opencode.db")
  const connection = openConnection({ filename: databaseFilename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  Flag.OPENCODE_DB = databaseFilename
})

afterAll(() => {
  Flag.OPENCODE_DB = previousDatabasePath
  if (databaseDirectory !== undefined) fs.rmSync(databaseDirectory, { recursive: true, force: true })
})

function init(): SessionWorkerInit {
  return {
    snapshot: workerSnapshot(),
    env: { values: {} },
    role: "session",
    workerID: 7,
    sessionID: "ses_bootstrap_dispose",
    model: {
      providerID: "mock",
      modelID: "m",
      apiId: "m",
      baseUrl: "http://127.0.0.1:1/v1",
      apiKey: "k",
      capabilities: { temperature: true, reasoning: false },
      limit: { context: 128_000, output: 4096 },
    },
    capabilities: { sessionWorkerPorts: true },
  }
}

function fakeScope() {
  const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
  const scope = {
    postMessage(data: unknown, transfer?: readonly Transferable[]) {
      frames.push({ data, transfer: transfer ?? [] })
    },
    onmessage: null as ((event: { data: unknown }) => void) | null,
    addEventListener(_type: "message", _listener: (event: { data: unknown }) => void) {},
    removeEventListener() {},
  }
  return { frames, scope }
}

describe("bootstrapSessionWorker disposal hardening", () => {
  test("a throw after the DB opens runs the orderly teardown and rethrows", () => {
    const { frames, scope } = fakeScope()
    let writerDisposed = 0
    let hubProxyDisposed = 0
    const fault = new Error("bootstrap fault after the DB opened")

    let thrown: unknown
    try {
      bootstrapSessionWorker(init(), scope, {
        observeWriter: (writer: WorkerWriterClient) => {
          const dispose = writer.dispose.bind(writer)
          writer.dispose = () => {
            writerDisposed += 1
            dispose()
          }
        },
        observeHubProxy: (client) => {
          const dispose = client.dispose.bind(client)
          client.dispose = () => {
            hubProxyDisposed += 1
            dispose()
          }
        },
        // The latest mid-bootstrap step before the handle is returned: a throw
        // here models any failure in that window (executor install, graph
        // construction) after the DB/writer/clients already exist.
        installExecutor: () => {
          throw fault
        },
      })
    } catch (error) {
      thrown = error
    } finally {
      for (const frame of frames) {
        for (const value of Object.values(frame.data as Record<string, unknown>)) {
          if (value instanceof MessagePort) value.close()
        }
      }
    }

    // The original failure surfaces; bootstrap must not swallow it.
    expect(thrown).toBe(fault)
    // The same orderly teardown as `dispose()` ran: both outbound clients were
    // disposed and the native handle was closed. SQLite removes the WAL/SHM
    // files when the last connection closes, so their absence proves the
    // mid-bootstrap throw no longer leaks the handle/WAL state.
    expect(writerDisposed).toBe(1)
    expect(hubProxyDisposed).toBe(1)
    expect(fs.existsSync(`${databaseFilename}-wal`)).toBe(false)
    expect(fs.existsSync(`${databaseFilename}-shm`)).toBe(false)
  })
})
