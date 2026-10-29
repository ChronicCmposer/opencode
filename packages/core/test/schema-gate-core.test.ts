// The cross-process schema gate lives at the core `Database` layer, not only in
// the session hub: every process that constructs the layer takes the sidecar
// lease before any migration statement runs. These tests pin the gate, the
// lease renewal loop, and release on interruption.

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { Database } from "../src/database/database"
import { openConnection, type Connection } from "../src/database/connection"
import {
  SchemaGateLostError,
  SchemaGateTimeoutError,
  acquireSchemaGate,
  newGateToken,
  openSchemaGate,
  releaseSchemaGate,
  schemaGateFilename,
  waitForSchemaGate,
  withSchemaGate,
} from "../src/database/schema-gate"
import { tmpdir } from "./fixture/tmpdir"

function tableNames(filename: string): readonly string[] {
  const connection = openConnection({ filename })
  try {
    return connection
      .all<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .map((row) => row.name)
  } finally {
    connection.close()
  }
}

function journalIDs(filename: string): readonly string[] {
  const connection = openConnection({ filename })
  try {
    return connection.all<{ id: string }>("SELECT id FROM migration ORDER BY id").map((row) => row.id)
  } finally {
    connection.close()
  }
}

describe("core database schema gate", () => {
  test("the Database layer waits for the cross-process gate before migrating", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "gated.sqlite")
    const gate = openSchemaGate(filename)
    const token = newGateToken()
    waitForSchemaGate(gate, token)

    const build = Effect.runPromise(Effect.scoped(Layer.build(Database.layerFromPath(filename))))

    try {
      // No fixed sleep: assert the positive outcome only after releasing the
      // lock. The dedicated `withSchemaGate` test below pins the wait itself
      // on the published acquisition signal.
      expect(tableNames(filename)).not.toContain("session")
    } finally {
      releaseSchemaGate(gate, token)
      gate.close()
    }

    await build
    expect(tableNames(filename)).toContain("session")
    expect(journalIDs(filename).length).toBeGreaterThan(0)
    expect(await Bun.file(schemaGateFilename(filename)).exists()).toBe(true)
  }, 30_000)

  test("withSchemaGate does not run work until it owns the lease", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "signal.sqlite")
    const gate = openSchemaGate(filename)
    const token = newGateToken()
    waitForSchemaGate(gate, token)

    let resolveWait!: () => void
    const waiting = new Promise<void>((resolve) => {
      resolveWait = resolve
    })
    let started = false
    const work = Effect.runPromise(
      withSchemaGate(
        filename,
        () =>
          Effect.sync(() => {
            started = true
          }),
        { pollMs: 5, onWaitForLease: resolveWait },
      ),
    )

    try {
      await waiting
      expect(started).toBe(false)
    } finally {
      releaseSchemaGate(gate, token)
      gate.close()
    }

    await work
    expect(started).toBe(true)
  }, 30_000)

  test("two concurrent layer builds serialize into one complete journal", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "concurrent.sqlite")
    const layers = [Database.layerFromPath(filename), Database.layerFromPath(filename)]

    await Effect.runPromise(
      Effect.all(
        layers.map((layer) => Effect.scoped(Layer.build(layer))),
        { concurrency: "unbounded" },
      ),
    )

    const ids = journalIDs(filename)
    expect(tableNames(filename)).toContain("session")
    expect(new Set(ids).size).toBe(ids.length)
  }, 30_000)

  test("renews the lease so a migration longer than the TTL keeps the gate", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "renew.sqlite")
    const competitor = openSchemaGate(filename)
    let blocked = false

    await Effect.runPromise(
      withSchemaGate(
        filename,
        () =>
          Effect.gen(function* () {
            yield* Effect.sleep(250)
            blocked = !acquireSchemaGate(competitor, newGateToken())
          }),
        { ttlMs: 80, renewIntervalMs: 20, pollMs: 5 },
      ),
    )
    competitor.close()

    expect(blocked).toBe(true)

    const after = openSchemaGate(filename)
    const token = newGateToken()
    try {
      expect(acquireSchemaGate(after, token)).toBe(true)
      releaseSchemaGate(after, token)
    } finally {
      after.close()
    }
  }, 10_000)

  test("releases the gate when the migration is interrupted", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "interrupt.sqlite")

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>()
          const fiber = yield* withSchemaGate(filename, () =>
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
          ).pipe(Effect.forkScoped)
          yield* Deferred.await(started)
          yield* Fiber.interrupt(fiber)
        }),
      ),
    )

    const after = openSchemaGate(filename)
    const token = newGateToken()
    try {
      expect(acquireSchemaGate(after, token)).toBe(true)
      releaseSchemaGate(after, token)
    } finally {
      after.close()
    }
  }, 10_000)

  test("a stolen lease aborts the work with SchemaGateLostError", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "lost.sqlite")
    const thief = openSchemaGate(filename)

    try {
      const exit = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const started = yield* Deferred.make<void>()
            const fiber = yield* withSchemaGate(
              filename,
              () =>
                Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
              { ttlMs: 200, renewIntervalMs: 10, pollMs: 5 },
            ).pipe(Effect.forkScoped)
            yield* Deferred.await(started)
            // Another process takes the row while the owner is still working.
            thief.run("UPDATE schema_gate SET owner = 'thief', expires_at = ? WHERE id = 1", [
              Date.now() + 60_000,
            ])
            return yield* Fiber.await(fiber)
          }),
        ),
      )

      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) {
        const lost = Cause.squash(exit.cause)
        expect(lost).toBeInstanceOf(SchemaGateLostError)
        expect((lost as SchemaGateLostError)._tag).toBe("WorkerDb.SchemaGateLost")
        expect((lost as SchemaGateLostError).tag).toBe("WorkerDb.SchemaGateLost")
      }
    } finally {
      thief.close()
    }
  }, 10_000)

  test("gate errors are tagged and keep their legacy tags and messages", () => {
    const timeout = new SchemaGateTimeoutError()
    expect(timeout._tag).toBe("WorkerDb.SchemaGateTimeout")
    expect(timeout.tag).toBe("WorkerDb.SchemaGateTimeout")
    expect(timeout.name).toBe("SchemaGateTimeoutError")
    expect(timeout.message).toBe("timed out waiting for the cross-process schema gate")
    expect(timeout).toBeInstanceOf(Error)

    const cause = new Error("renew failed")
    const lost = new SchemaGateLostError({ cause })
    expect(lost._tag).toBe("WorkerDb.SchemaGateLost")
    expect(lost.tag).toBe("WorkerDb.SchemaGateLost")
    expect(lost.name).toBe("SchemaGateLostError")
    expect(lost.message).toBe("the cross-process schema gate lease was lost before completion")
    expect(lost.cause).toBe(cause)
    expect(lost).toBeInstanceOf(Error)
  })

  test("a successful migration releases the lease", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "released.sqlite")

    await Effect.runPromise(withSchemaGate(filename, () => Effect.void, { pollMs: 5 }))

    const after = openSchemaGate(filename)
    const token = newGateToken()
    try {
      expect(acquireSchemaGate(after, token)).toBe(true)
      releaseSchemaGate(after, token)
    } finally {
      after.close()
    }
  }, 10_000)

  test("closes the sidecar connection when acquisition is interrupted", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "acquisition.sqlite")
    const holder = openSchemaGate(filename)
    const holderToken = newGateToken()
    waitForSchemaGate(holder, holderToken)

    let opened: Connection | undefined
    let resolveWait!: () => void
    const waiting = new Promise<void>((resolve) => {
      resolveWait = resolve
    })

    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const fiber = yield* withSchemaGate(filename, () => Effect.void, {
              pollMs: 1_000,
              timeoutMs: 60_000,
              onWaitForLease: resolveWait,
              onLeaseOpened: (connection) => {
                opened = connection
              },
            }).pipe(Effect.forkScoped)
            yield* Effect.promise(() => waiting)
            yield* Fiber.interrupt(fiber)
          }),
        ),
      )
    } finally {
      releaseSchemaGate(holder, holderToken)
      holder.close()
    }

    expect(opened).toBeDefined()
    expect(opened!.closed).toBe(true)
  }, 10_000)
})

describe("schema gate release maintenance", () => {
  test("retries a busy DELETE before giving up", () => {
    let deleteAttempts = 0
    const calls: string[] = []
    const connection: Pick<Connection, "run"> = {
      run(sql: string) {
        calls.push(sql)
        if (sql.startsWith("DELETE")) {
          deleteAttempts += 1
          if (deleteAttempts < 3) throw { code: "SQLITE_BUSY" }
        }
        return { changes: 1, lastInsertRowid: 0 }
      },
    }

    releaseSchemaGate(connection, newGateToken())

    expect(deleteAttempts).toBe(3)
    expect(calls.filter((sql) => sql.startsWith("UPDATE"))).toEqual([])
  })

  test("falls back to expiring the row when DELETE keeps failing", () => {
    let updates = 0
    const connection: Pick<Connection, "run"> = {
      run(sql: string) {
        if (sql.startsWith("DELETE")) throw { code: "SQLITE_BUSY" }
        if (sql.startsWith("UPDATE")) updates += 1
        return { changes: 1, lastInsertRowid: 0 }
      },
    }

    releaseSchemaGate(connection, newGateToken())

    expect(updates).toBe(1)
  })

  test("logs a warning when both the DELETE and the expiry write fail", () => {
    const warnings: string[] = []
    const original = console.warn
    console.warn = (message: string) => {
      warnings.push(message)
    }
    const connection: Pick<Connection, "run"> = {
      run() {
        throw { code: "SQLITE_BUSY" }
      },
    }

    try {
      expect(() => releaseSchemaGate(connection, newGateToken())).not.toThrow()
    } finally {
      console.warn = original
    }

    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toContain("failed to release lease")
  })
})
