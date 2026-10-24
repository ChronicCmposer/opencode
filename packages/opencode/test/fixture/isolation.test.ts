import { describe, expect, test } from "bun:test"
import os from "os"
import path from "path"
import { assertIsolatedDatabasePath, isInsideTemp } from "./isolation"

// The production database lives at `$XDG_DATA_HOME/opencode/opencode.db`, which
// defaults to `~/.local/share`. Derive it from the home directory rather than a
// hardcoded absolute path so the guard travels with the host.
const productionDatabase = path.join(os.homedir(), ".local", "share", "opencode", "opencode.db")

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
