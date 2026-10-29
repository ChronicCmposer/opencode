// b-core — bounded busy-retry for the main SQLite connection.
//
// The main-process read/write connection contends with the worker writer isolate.
// When a lock is held longer than `busy_timeout`, the driver surfaces a busy
// failure (`LockTimeoutError` reason / `SQLITE_BUSY` / "database is locked").
// `withBusyRetry` recovers with bounded exponential backoff and still fails loud
// once the budget is spent. These tests pin the classifier, the retry control
// flow, the exhaustion error, and the environment config.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Database as BunDatabase } from "bun:sqlite"
import { afterEach, describe, expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { SqlClient } from "effect/unstable/sql/SqlClient"
import { layer as reactivityLayer } from "effect/unstable/reactivity/Reactivity"
import { LockTimeoutError, SqlError } from "effect/unstable/sql/SqlError"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Credential } from "@opencode-ai/core/credential"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Integration } from "@opencode-ai/core/integration"
import {
  BusyExhaustedError,
  busyDelayMs,
  isBusySqlError,
  isNativeSqliteError,
  resolveBusyRetryConfig,
  withBusyRetry,
} from "@opencode-ai/core/database/busy"
import {
  makeSqliteClient,
  nextTransactionState,
  transactionControl,
  type SqliteNativeRunner,
} from "@opencode-ai/core/database/sqlite-adapter"
import { ConnectionClosedError } from "@opencode-ai/core/database/connection"
import {
  TransactionPoisonedError,
  TransactionRollbackError,
  inImmediateTransaction,
  isPoisoned,
  type TransactionConnection,
} from "@opencode-ai/core/database/transaction"

const lockTimeoutSqlError = () =>
  new SqlError({ reason: new LockTimeoutError({ cause: new Error("database is locked") }) })

const busyFailure = { code: "SQLITE_BUSY" } as const
const nonBusyFailure = new Error("no such table: users")

const directories: string[] = []
const natives: BunDatabase[] = []

afterEach(() => {
  for (const native of natives.splice(0)) native.close()
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("busy classifier", () => {
  test("recognizes a SqlError whose reason is a LockTimeoutError", () => {
    expect(isBusySqlError(lockTimeoutSqlError())).toBe(true)
  })

  test("recognizes SQLITE_BUSY and SQLITE_LOCKED codes", () => {
    expect(isBusySqlError({ code: "SQLITE_BUSY" })).toBe(true)
    expect(isBusySqlError({ code: "SQLITE_LOCKED" })).toBe(true)
    // The writer-side classifier also reads `sqliteCode`; mirror that here.
    expect(isBusySqlError({ sqliteCode: "SQLITE_BUSY" })).toBe(true)
  })

  test("prefix-matches the extended SQLITE_BUSY_*/SQLITE_LOCKED_* codes (R5#5)", () => {
    expect(isBusySqlError({ code: "SQLITE_BUSY_SNAPSHOT" })).toBe(true)
    expect(isBusySqlError({ code: "SQLITE_BUSY_RECOVERY" })).toBe(true)
    expect(isBusySqlError({ code: "SQLITE_LOCKED_SHAREDCACHE" })).toBe(true)
    expect(isBusySqlError({ sqliteCode: "SQLITE_BUSY_SNAPSHOT" })).toBe(true)
    // A longer word that merely shares the prefix (no underscore boundary) is not busy.
    expect(isBusySqlError({ code: "SQLITE_BUSYNESS" })).toBe(false)
  })

  test("recognizes a message containing database is locked on a native driver error", () => {
    expect(isBusySqlError({ name: "SQLiteError", code: "SQLITE_BUSY", message: "database is locked" })).toBe(true)
    expect(isBusySqlError({ name: "SQLiteError", errno: 5, message: "SQLITE_BUSY: database is locked" })).toBe(true)
  })

  test("does not treat a bare message on an arbitrary error as busy (R5#6)", () => {
    // Without a native-driver shape, a message match is too weak to retry on.
    expect(isBusySqlError(new Error("database is locked"))).toBe(false)
    expect(isBusySqlError(new Error("SQLITE_BUSY: database is locked"))).toBe(false)
  })

  test("rejects non-busy errors and non-objects", () => {
    expect(isBusySqlError(nonBusyFailure)).toBe(false)
    expect(isBusySqlError({ code: "SQLITE_CONSTRAINT" })).toBe(false)
    expect(isBusySqlError(undefined)).toBe(false)
    expect(isBusySqlError("SQLITE_BUSY")).toBe(false)
  })
})

describe("isNativeSqliteError", () => {
  test("narrows exactly the driver shapes the classifier may trust", () => {
    expect(isNativeSqliteError({ name: "SQLiteError" })).toBe(true)
    expect(isNativeSqliteError({ code: "SQLITE_BUSY" })).toBe(true)
    expect(isNativeSqliteError({ code: 5 })).toBe(true)
    expect(isNativeSqliteError({ errno: 5 })).toBe(true)
    expect(isNativeSqliteError({ sqliteCode: "SQLITE_BUSY" })).toBe(true)
    // An arbitrary error whose only claim is its message is not a driver error.
    expect(isNativeSqliteError(new Error("database is locked"))).toBe(false)
    expect(isNativeSqliteError("SQLITE_BUSY")).toBe(false)
    expect(isNativeSqliteError(null)).toBe(false)
  })
})

describe("withBusyRetry", () => {
  test("retries a busy failure until it succeeds", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return attempts <= 2 ? Effect.fail(busyFailure) : Effect.succeed("committed")
    })

    const result = await Effect.runPromise(withBusyRetry(effect, { retries: 5, baseDelayMs: 1, maxDelayMs: 4 }))

    expect(result).toBe("committed")
    expect(attempts).toBe(3)
  })

  test("backoff doubles and caps at maxDelayMs", () => {
    const config = { retries: 5, baseDelayMs: 10, maxDelayMs: 25 }
    expect([1, 2, 3, 4].map((failed) => busyDelayMs(config, failed))).toEqual([10, 20, 25, 25])
  })

  test("exhausts with a typed BusyExhaustedError carrying attempts and last cause", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(busyFailure)
    })

    const error = await Effect.runPromise(
      withBusyRetry(effect, { retries: 2, baseDelayMs: 1, maxDelayMs: 2 }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(BusyExhaustedError)
    const exhausted = error as BusyExhaustedError
    expect(exhausted.attempts).toBe(3)
    expect(exhausted.lastCause).toEqual(busyFailure)
    expect(attempts).toBe(3)
  })

  test("retries: 0 exhausts on the first busy failure with a typed failure", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(busyFailure)
    })

    const error = await Effect.runPromise(
      withBusyRetry(effect, { retries: 0, baseDelayMs: 1, maxDelayMs: 1 }).pipe(Effect.flip),
    )

    expect(error).toBeInstanceOf(BusyExhaustedError)
    expect(attempts).toBe(1)
  })

  test("does not retry non-busy failures and passes the typed error through", async () => {
    let attempts = 0
    const effect = Effect.suspend(() => {
      attempts += 1
      return Effect.fail(nonBusyFailure)
    })

    const error = await Effect.runPromise(
      withBusyRetry(effect, { retries: 5, baseDelayMs: 1, maxDelayMs: 1 }).pipe(Effect.flip),
    )

    expect(error).toBe(nonBusyFailure)
    expect(attempts).toBe(1)
  })

  test("a successful attempt passes through unchanged", async () => {
    const result = await Effect.runPromise(withBusyRetry(Effect.succeed(42), {}))
    expect(result).toBe(42)
  })
})

describe("resolveBusyRetryConfig", () => {
  test("applies defaults when the environment is empty", () => {
    expect(resolveBusyRetryConfig({})).toEqual({ retries: 5, baseDelayMs: 50, maxDelayMs: 1000 })
  })

  test("parses overrides and falls back on malformed values", () => {
    expect(
      resolveBusyRetryConfig({
        OPENCODE_DB_BUSY_RETRIES: "9",
        OPENCODE_DB_BUSY_BASE_DELAY_MS: "5",
        OPENCODE_DB_BUSY_MAX_DELAY_MS: "250",
      }),
    ).toEqual({ retries: 9, baseDelayMs: 5, maxDelayMs: 250 })

    expect(
      resolveBusyRetryConfig({
        OPENCODE_DB_BUSY_RETRIES: "not-a-number",
        OPENCODE_DB_BUSY_BASE_DELAY_MS: "-1",
        OPENCODE_DB_BUSY_MAX_DELAY_MS: "3.5",
      }),
    ).toEqual({ retries: 5, baseDelayMs: 50, maxDelayMs: 1000 })
  })
})

describe("transaction state tracking (F18)", () => {
  const outside = { inTransaction: false, savepointDepth: 0, openedByBegin: false }

  test("classifies RELEASE and ROLLBACK TO as distinct controls", () => {
    expect(transactionControl("RELEASE savepoint effect_sql_1")).toBe("release")
    expect(transactionControl("ROLLBACK TO SAVEPOINT effect_sql_1")).toBe("rollbackTo")
    expect(transactionControl("ROLLBACK TO effect_sql_1")).toBe("rollbackTo")
    expect(transactionControl("SAVEPOINT s")).toBe("savepoint")
    expect(transactionControl("COMMIT")).toBe("commit")
    expect(transactionControl("SELECT 1")).toBeUndefined()
  })

  test("a top-level SAVEPOINT opens a transaction and its RELEASE closes it", () => {
    const opened = nextTransactionState(outside, "savepoint")
    expect(opened).toEqual({ inTransaction: true, savepointDepth: 1, openedByBegin: false })
    expect(nextTransactionState(opened, "release")).toEqual(outside)
  })

  test("nested savepoints stay inside; ROLLBACK TO keeps the savepoint and transaction open", () => {
    const outer = nextTransactionState(outside, "savepoint")
    const inner = nextTransactionState(outer, "savepoint")
    expect(inner.savepointDepth).toBe(2)

    const rewound = nextTransactionState(inner, "rollbackTo")
    expect(rewound).toEqual(inner)

    const releasedInner = nextTransactionState(rewound, "release")
    expect(releasedInner).toEqual(outer)
    expect(nextTransactionState(releasedInner, "release")).toEqual(outside)
  })

  test("releasing a savepoint inside BEGIN keeps the transaction open", () => {
    const begun = nextTransactionState(outside, "begin")
    const savepoint = nextTransactionState(begun, "savepoint")
    const released = nextTransactionState(savepoint, "release")
    expect(released).toEqual(begun)
    expect(nextTransactionState(released, "commit")).toEqual(outside)
  })

  test("top-level ROLLBACK leaves even while savepoints are open", () => {
    const outer = nextTransactionState(outside, "savepoint")
    const inner = nextTransactionState(outer, "savepoint")
    expect(nextTransactionState(inner, "rollback")).toEqual(outside)
  })
})

describe("busy ROLLBACK recovery", () => {
  test("a busy ROLLBACK is retried in place instead of poisoning the connection", () => {
    const statements: string[] = []
    let rollbacks = 0
    const connection: TransactionConnection = {
      exec(sql) {
        statements.push(sql)
        if (sql !== "ROLLBACK") return
        rollbacks += 1
        if (rollbacks === 1) throw busyFailure
      },
    }

    let failure: unknown
    try {
      inImmediateTransaction(
        connection,
        () => {
          throw nonBusyFailure
        },
        { retries: 2 },
      )
    } catch (error) {
      failure = error
    }

    // The body failed once and was not replayed; only ROLLBACK was retried.
    expect(failure).toBe(nonBusyFailure)
    expect(rollbacks).toBe(2)
    expect(statements.filter((sql) => sql === "BEGIN IMMEDIATE")).toHaveLength(1)
    expect(isPoisoned(connection)).toBe(false)
  })

  test("an exhausted busy ROLLBACK poisons the connection and is typed", () => {
    let rollbacks = 0
    const connection: TransactionConnection = {
      exec(sql) {
        if (sql !== "ROLLBACK") return
        rollbacks += 1
        throw busyFailure
      },
    }

    let failure: unknown
    try {
      inImmediateTransaction(
        connection,
        () => {
          throw nonBusyFailure
        },
        { retries: 1 },
      )
    } catch (error) {
      failure = error
    }

    expect(failure).toBeInstanceOf(TransactionRollbackError)
    expect((failure as TransactionRollbackError)._tag).toBe("Database.TransactionRollback")
    expect(rollbacks).toBe(2)
    expect(isPoisoned(connection)).toBe(true)
  })
})

describe("database error idiom", () => {
  test("busy exhaustion keeps its name, legacy tag, fields, and message", () => {
    const error = new BusyExhaustedError(4, busyFailure)

    expect(error._tag).toBe("Database.BusyExhausted")
    expect(error.tag).toBe("SQLITE_BUSY_EXHAUSTED")
    expect(error.name).toBe("BusyExhaustedError")
    expect(error.message).toBe("SQLite remained busy after 4 attempt(s)")
    expect(error.attempts).toBe(4)
    expect(error.lastCause).toBe(busyFailure)
    expect(error).toBeInstanceOf(Error)
  })

  test("rollback and poisoned errors keep their typed identity", () => {
    const cause = new Error("commit failed")
    const rollbackCause = new Error("disk gone")
    const rollback = new TransactionRollbackError(cause, rollbackCause)

    expect(rollback._tag).toBe("Database.TransactionRollback")
    expect(rollback.tag).toBe("DB_CORE_ROLLBACK_FAILED")
    expect(rollback.name).toBe("TransactionRollbackError")
    expect(rollback.message).toBe("transaction rollback failed; connection is poisoned")
    expect(rollback.cause).toBe(cause)
    expect(rollback.rollbackCause).toBe(rollbackCause)
    expect(rollback).toBeInstanceOf(Error)

    const poisoned = new TransactionPoisonedError()
    expect(poisoned._tag).toBe("Database.TransactionPoisoned")
    expect(poisoned.tag).toBe("DB_CORE_CONNECTION_POISONED")
    expect(poisoned.name).toBe("TransactionPoisonedError")
    expect(poisoned.message).toBe("database connection is poisoned by a failed rollback")
    expect(poisoned).toBeInstanceOf(Error)
  })

  test("a closed connection reports the filename with a namespaced tag", () => {
    const error = new ConnectionClosedError("/tmp/closed.sqlite")

    expect(error._tag).toBe("Database.ConnectionClosed")
    expect(error.tag).toBe("DB_CORE_CONNECTION_CLOSED")
    expect(error.name).toBe("ConnectionClosedError")
    expect(error.message).toBe("database connection is closed: /tmp/closed.sqlite")
    expect(error.filename).toBe("/tmp/closed.sqlite")
    expect(error).toBeInstanceOf(Error)
  })
})

const CREDENTIAL_DDL = `
  CREATE TABLE credential (
    id text PRIMARY KEY,
    integration_id text,
    label text NOT NULL,
    value text NOT NULL,
    connector_id text,
    method_id text,
    active integer,
    time_created integer NOT NULL,
    time_updated integer NOT NULL
  );
`

/**
 * A real adapter-backed `Database.Service` over one file. The runner reports the
 * first busy `BEGIN` through `onBeginBusy` so the test can release the lock at a
 * published point instead of sleeping.
 */
function credentialDatabaseLayer(filename: string, onBeginBusy: () => void): Layer.Layer<DatabaseService.Service> {
  const native = new BunDatabase(filename, { create: true })
  native.exec("PRAGMA journal_mode = WAL")
  native.exec("PRAGMA busy_timeout = 0")
  natives.push(native)

  let reported = false
  const runner: SqliteNativeRunner = {
    all: (query, params, safeIntegers) => {
      try {
        const statement = native.query(query)
        statement.safeIntegers(safeIntegers)
        return (statement.all(...(params as never[])) ?? []) as Array<Record<string, unknown>>
      } catch (cause) {
        if (!reported && transactionControl(query) === "begin") {
          reported = true
          onBeginBusy()
        }
        throw cause
      }
    },
    values: (query, params, safeIntegers) => {
      const statement = native.query(query)
      statement.safeIntegers(safeIntegers)
      return (statement.values(...(params as never[])) ?? []) as ReadonlyArray<ReadonlyArray<unknown>>
    },
  }

  return Layer.effect(
    DatabaseService.Service,
    Effect.gen(function* () {
      const client = Object.assign(yield* makeSqliteClient({}, runner), { config: { filename } })
      const db = yield* EffectDrizzleSqlite.makeWithDefaults().pipe(Effect.provideService(SqlClient, client))
      return { db }
    }),
  ).pipe(Layer.provide(reactivityLayer))
}

describe("non-EventV2 caller under a writer lock", () => {
  test("Credential.create retries a busy BEGIN and commits exactly one row", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-credential-busy-"))
    directories.push(directory)
    const filename = path.join(directory, "credential.db")

    const holder = new BunDatabase(filename, { create: true })
    natives.push(holder)
    holder.exec("PRAGMA journal_mode = WAL")
    holder.exec("PRAGMA busy_timeout = 0")
    holder.exec(CREDENTIAL_DDL)
    holder.exec("BEGIN IMMEDIATE")

    let signalBusy!: () => void
    const beginBusy = new Promise<void>((resolve) => {
      signalBusy = resolve
    })

    const layer = LayerNode.compile(Credential.node, [
      [DatabaseService.node, credentialDatabaseLayer(filename, signalBusy)] as const,
    ])
    const create = Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const credentials = yield* Credential.Service
          return yield* credentials.create({
            integrationID: Integration.ID.make("openai"),
            label: "Contended",
            value: Credential.Key.make({ type: "key", key: "secret" }),
          })
        }).pipe(Effect.provide(layer)),
      ),
    )
    const settled = create.then(
      () => ({ tag: "settled" as const }),
      (error: unknown) => ({ tag: "failed" as const, error }),
    )
    const first = await Promise.race([beginBusy.then(() => ({ tag: "busy" as const })), settled])
    if (first.tag !== "busy") {
      throw new Error(`Credential.create did not reach a busy BEGIN first: ${JSON.stringify(first)}`)
    }

    // Release the write lock inside the bounded retry budget.
    holder.exec("COMMIT")
    const created = await create

    expect(holder.query("SELECT COUNT(*) AS count FROM credential").get()).toEqual({ count: 1 })
    expect(holder.query("SELECT id FROM credential").get()).toEqual({ id: created.id })
  })
})
