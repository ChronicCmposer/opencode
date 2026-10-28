// Boot the real `session-worker.ts` entry and observe its control frames.
//
// Contract: the production entry installs the executor on the default worker
// channel and announces `ready`. A full hub-port boot transfers its relay ports
// (`ports` then `hub-proxy`) before that handshake; a degraded boot sends only
// `ready`. The harness surfaces all of them plus an `ExecutorTransport` so tests
// can drive the isolate the way the hub does.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { openConnection } from "@opencode-ai/core/database/connection"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity } from "@/session/worker/db"
import { bootSessionWorker } from "./session-worker-harness"
import { sessionWorkerInit } from "./session-worker-init"

describe("session worker harness", () => {
  test("boots the production entry and observes the ready handshake", async () => {
    const handle = await bootSessionWorker({ timeoutMs: 20_000 })
    try {
      expect(handle.ready.type).toBe("ready")
      // `threadId` is 0 on the hub; a positive id proves the entry ran in an isolate.
      expect(handle.ready.threadId).toBeGreaterThan(0)
      // The default init is the degraded boot: no hub-served ports are transferred.
      expect(handle.ports).toBeUndefined()
      expect(handle.hubProxyPort).toBeUndefined()
      expect(handle.port.alive).toBe(true)
    } finally {
      await handle.terminate()
    }

    expect(handle.port.alive).toBe(false)
  })

  test("observes the transferred relay and hub-proxy ports on a full boot", async () => {
    // A full boot opens the worker's per-worker WAL connection at
    // `Database.path()`; point it at a migrated file (the suite default is an
    // unmigrated `:memory:` database).
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-harness-db-"))
    const filename = path.join(directory, "opencode.db")
    const connection = openConnection({ filename })
    try {
      for (const statement of migrationStatements) connection.exec(statement)
      markSchemaIdentity(connection)
    } finally {
      connection.close()
    }
    const handle = await bootSessionWorker({
      timeoutMs: 20_000,
      env: { OPENCODE_DB: filename },
      init: { ...sessionWorkerInit(), capabilities: { sessionWorkerPorts: true } },
    })
    try {
      expect(handle.ready.type).toBe("ready")
      expect(handle.ports?.egress).toBeInstanceOf(MessagePort)
      expect(handle.ports?.egressControl).toBeInstanceOf(MessagePort)
      expect(handle.hubProxyPort).toBeInstanceOf(MessagePort)
    } finally {
      await handle.terminate()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })

  test("round-trips an executor ping/pong frame over the transport", async () => {
    const handle = await bootSessionWorker({ timeoutMs: 20_000 })
    try {
      handle.port.post({ type: "ping", id: 42 })
      const message = await handle.port.waitFor((m) => m.type === "pong")
      expect(message).toEqual({ type: "pong", id: 42 })
    } finally {
      await handle.terminate()
    }
  })

  test("accepts a caller-supplied init payload", async () => {
    const handle = await bootSessionWorker({
      timeoutMs: 20_000,
      init: sessionWorkerInit({ env: { values: { OPENCODE_TEST_WORKER_KEY: "harness" } } }),
    })
    try {
      expect(handle.ready.threadId).toBeGreaterThan(0)
    } finally {
      await handle.terminate()
    }
  })

  test("supports `await using` disposal", async () => {
    let port: Awaited<ReturnType<typeof bootSessionWorker>>["port"] | undefined
    {
      await using handle = await bootSessionWorker({ timeoutMs: 20_000 })
      port = handle.port
      expect(handle.port.alive).toBe(true)
    }
    expect(port?.alive).toBe(false)
  })
})
