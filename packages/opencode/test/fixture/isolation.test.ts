import { describe, expect, test } from "bun:test"
import os from "os"
import path from "path"
import { assertIsolatedDatabasePath, isInsideTemp } from "./isolation"

const productionDatabase = "/var/lib/opencode/.local/share/opencode/opencode.db"

describe("isInsideTemp", () => {
  test("accepts a path directly under the OS temp directory", async () => {
    expect(await isInsideTemp(path.join(os.tmpdir(), "x.db"))).toBe(true)
  })

  test("accepts a nested path under the OS temp directory", async () => {
    expect(await isInsideTemp(path.join(os.tmpdir(), "a", "b", "x.db"))).toBe(true)
  })

  test("rejects a path under the home directory", async () => {
    expect(await isInsideTemp(path.join(os.homedir(), "x.db"))).toBe(false)
  })

  test("rejects the production database path", async () => {
    expect(await isInsideTemp(productionDatabase)).toBe(false)
  })

  test("rejects a sibling sharing a string prefix with the temp directory", async () => {
    expect(await isInsideTemp(path.join(os.tmpdir() + "-sibling", "x.db"))).toBe(false)
  })
})

describe("assertIsolatedDatabasePath", () => {
  test("allows :memory:", async () => {
    await expect(assertIsolatedDatabasePath(":memory:")).resolves.toBeUndefined()
  })

  test("rejects a non-temp path and names the offending path", async () => {
    await expect(assertIsolatedDatabasePath(productionDatabase)).rejects.toThrow(productionDatabase)
  })
})
