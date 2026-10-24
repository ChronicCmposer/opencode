import { describe, expect, test } from "bun:test"
import { rm, stat, writeFile } from "fs/promises"
import os from "os"
import path from "path"
import { Flag } from "@opencode-ai/core/flag/flag"
import { resetDatabase } from "./db"

function randomSuffix(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36)
}

async function exists(target: string): Promise<boolean> {
  return stat(target).then(
    () => true,
    () => false,
  )
}

async function removeQuietly(...targets: string[]): Promise<void> {
  for (const target of targets) await rm(target, { force: true }).catch(() => undefined)
}

/**
 * Run `body` with `Flag.OPENCODE_DB` pinned to `dbPath`, restoring the previous
 * value afterwards. Each test owns its flag lifetime explicitly instead of
 * relying on a shared `afterEach`.
 */
async function withDatabase(dbPath: string, body: () => Promise<void>): Promise<void> {
  const previous = Flag.OPENCODE_DB
  Flag.OPENCODE_DB = dbPath
  try {
    await body()
  } finally {
    Flag.OPENCODE_DB = previous
  }
}

describe("resetDatabase", () => {
  test("deletes a temp database together with its wal/shm sidecars", async () => {
    const base = path.join(os.tmpdir(), `opencode-reset-test-${randomSuffix()}.db`)
    const files = [base, `${base}-wal`, `${base}-shm`]
    for (const file of files) await writeFile(file, "fixture")

    await withDatabase(base, async () => {
      await resetDatabase()
      for (const file of files) expect(await exists(file)).toBe(false)
    })
  })

  test("refuses to touch a database outside the temp dir", async () => {
    const sentinel = path.join(os.homedir(), `.opencode-reset-sentinel-${randomSuffix()}.db`)
    await writeFile(sentinel, "do-not-delete")

    try {
      await withDatabase(sentinel, async () => {
        await expect(resetDatabase()).rejects.toThrow(/refusing to operate on a non-temp database/)
      })
      expect(await exists(sentinel)).toBe(true)
    } finally {
      await removeQuietly(sentinel)
    }
  })

  test("treats :memory: as a no-op", async () => {
    await withDatabase(":memory:", async () => {
      await expect(resetDatabase()).resolves.toBeUndefined()
    })
  })
})
