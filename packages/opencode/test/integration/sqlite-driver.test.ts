// SQLite driver identity and busy classification.
//
// The process opens `bun:sqlite` handles (hub, executor, worker WAL) and
// `node:sqlite` handles (the scoped worker boundary). A split would mean two
// SQLite libraries with independent locks, so the cross-handle lock tests below
// prove they are one library: a write lock taken by one driver is observed by
// the other, and both report the same `sqlite_version()`.
//
// They also pin busy classification for the chosen driver: `node:sqlite` reports
// `ERR_SQLITE_ERROR` with `errcode`/`errstr` rather than bun's `SQLITE_BUSY`
// code, and the retry classifiers must recognize both shapes.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Database as BunDatabase } from "bun:sqlite"
import { DatabaseSync } from "node:sqlite"
import { isBusySqlError, isNativeSqliteError } from "@opencode-ai/core/database/busy"
import { isBusyError } from "@opencode-ai/core/database/transaction"
import { scheduleCheckpoint } from "@/session/worker/db-executor"

const directories: string[] = []

function workspace(): { filename: string } {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-sqlite-driver-"))
  directories.push(directory)
  return { filename: path.join(directory, "driver.db") }
}

function busyError(run: () => void): unknown {
  try {
    run()
  } catch (error) {
    return error
  }
  throw new Error("expected the statement to fail with a lock error")
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("one SQLite library for both drivers", () => {
  test("both drivers report the same SQLite version", () => {
    const { filename } = workspace()
    const bun = new BunDatabase(filename)
    const node = new DatabaseSync(filename)
    try {
      const bunVersion = bun.query("SELECT sqlite_version() AS version").get() as { version: string }
      const nodeVersion = node.prepare("SELECT sqlite_version() AS version").get() as { version: string }
      expect(nodeVersion.version).toBe(bunVersion.version)
    } finally {
      bun.close()
      node.close()
    }
  })

  test("a bun write lock is visible to a node handle (and vice versa)", () => {
    const { filename } = workspace()
    const bun = new BunDatabase(filename)
    const node = new DatabaseSync(filename)
    try {
      bun.exec("PRAGMA journal_mode = WAL")
      bun.exec("CREATE TABLE item (id INTEGER PRIMARY KEY, name TEXT)")
      node.exec("PRAGMA busy_timeout = 0")

      bun.exec("BEGIN IMMEDIATE")
      bun.exec("INSERT INTO item (name) VALUES ('bun')")
      expect(isNativeSqliteError(busyError(() => node.exec("INSERT INTO item (name) VALUES ('node')")))).toBe(true)
      bun.exec("ROLLBACK")

      node.exec("BEGIN IMMEDIATE")
      node.exec("INSERT INTO item (name) VALUES ('node')")
      bun.exec("PRAGMA busy_timeout = 0")
      expect(isNativeSqliteError(busyError(() => bun.exec("INSERT INTO item (name) VALUES ('bun')")))).toBe(true)
      node.exec("ROLLBACK")

      // A row committed by one handle is immediately visible to the other.
      node.exec("INSERT INTO item (name) VALUES ('committed')")
      expect(bun.query("SELECT name FROM item").all()).toEqual([{ name: "committed" }])
    } finally {
      bun.close()
      node.close()
    }
  })
})

describe("busy classification covers both driver error shapes", () => {
  test("a real node:sqlite busy error is classified as retryable", () => {
    const { filename } = workspace()
    const holder = new BunDatabase(filename)
    const node = new DatabaseSync(filename)
    try {
      holder.exec("PRAGMA journal_mode = WAL")
      holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY)")
      node.exec("PRAGMA busy_timeout = 0")
      holder.exec("BEGIN IMMEDIATE")

      const error = busyError(() => node.exec("INSERT INTO item DEFAULT VALUES"))
      expect(isNativeSqliteError(error)).toBe(true)
      expect(isBusySqlError(error)).toBe(true)
      expect(isBusyError(error)).toBe(true)
      holder.exec("ROLLBACK")
    } finally {
      holder.close()
      node.close()
    }
  })

  test("the SqlError wrapper around a node busy cause is retryable", async () => {
    const { filename } = workspace()
    const holder = new BunDatabase(filename)
    const node = new DatabaseSync(filename)
    try {
      holder.exec("PRAGMA journal_mode = WAL")
      holder.exec("CREATE TABLE item (id INTEGER PRIMARY KEY)")
      node.exec("PRAGMA busy_timeout = 0")
      holder.exec("BEGIN IMMEDIATE")
      const raw = busyError(() => node.exec("INSERT INTO item DEFAULT VALUES"))

      const { SqlError, classifySqliteError } = await import("effect/unstable/sql/SqlError")
      const wrapped = new SqlError({
        reason: classifySqliteError(raw, { message: "insert failed", operation: "execute" }),
      })
      expect(isBusySqlError(wrapped)).toBe(true)
      holder.exec("ROLLBACK")
    } finally {
      holder.close()
      node.close()
    }
  })
})

describe("periodic checkpoint", () => {
  test("runs a passive checkpoint on the configured cadence and stops on demand", async () => {
    const statements: string[] = []
    const stop = scheduleCheckpoint({ exec: (sql) => statements.push(sql) }, 10)
    try {
      await new Promise((resolve) => setTimeout(resolve, 45))
    } finally {
      stop()
    }
    expect(statements.length).toBeGreaterThan(0)
    expect(statements.every((sql) => sql === "PRAGMA wal_checkpoint(PASSIVE)")).toBe(true)

    const afterStop = statements.length
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(statements.length).toBe(afterStop)
  })

  test("a disabled cadence never schedules work", async () => {
    const statements: string[] = []
    const stop = scheduleCheckpoint({ exec: (sql) => statements.push(sql) }, 0)
    await new Promise((resolve) => setTimeout(resolve, 25))
    stop()
    expect(statements).toEqual([])
  })
})
