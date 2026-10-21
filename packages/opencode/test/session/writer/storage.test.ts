// Phase 7.13 — the writer serializes file-KV `Storage` (plan task 7.13).
//
// `Storage` is file-KV, not SQLite, so it needs a non-SQL mutation family on the
// writer. These tests cover the fs backend directly and the REAL worker isolate
// over the storage frames (openStorage / storage.read|write|remove|list).

import { describe, expect, test } from "bun:test"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"
import { createWriterClient } from "@/session/writer/client"
import { createBunSqliteConnection } from "@/session/writer/connection"
import { createWriterCore } from "@/session/writer/core"
import { createFileStorage } from "@/session/writer/storage"
import { tmpdir } from "../../fixture/fixture"
import { healthyEnv } from "./fixtures"

describe("atomic file-KV update (step 1)", () => {
  test("createFileStorage.update reads-modifies-writes and reports absence", async () => {
    await using tmp = await tmpdir()
    const storage = createFileStorage(path.join(tmp.path, "storage"))

    expect(await storage.update(["a"], () => {})).toEqual({ found: false })

    await storage.write(["a"], { n: 0 })
    const result = await storage.update(["a"], (content) => {
      ;(content as { n: number }).n += 1
    })
    expect(result).toEqual({ found: true, content: { n: 1 } })
    expect(await storage.read(["a"])).toEqual({ found: true, content: { n: 1 } })
  })

  test("concurrent writer updates serialize: no lost update", async () => {
    await using tmp = await tmpdir()
    const connection = createBunSqliteConnection({ filename: ":memory:" })
    const core = createWriterCore({ connection, storage: createFileStorage(path.join(tmp.path, "storage")) })

    await core.storageWrite(["counter"], { n: 0 })
    const bump = (content: unknown) => {
      ;(content as { n: number }).n += 1
    }
    await Promise.all([
      core.storageUpdate(["counter"], bump),
      core.storageUpdate(["counter"], bump),
      core.storageUpdate(["counter"], bump),
    ])

    expect(await core.storageRead(["counter"])).toEqual({ found: true, content: { n: 3 } })
    await core.close()
  })
})

describe("file-KV key safety (steps 22, 23)", () => {
  test("rejects path-traversal and empty keys", async () => {
    await using tmp = await tmpdir()
    const storage = createFileStorage(path.join(tmp.path, "storage"))

    await expect(storage.read(["..", "escape"])).rejects.toThrow(/invalid storage key segment|escapes/)
    await expect(storage.write(["a/b"], {})).rejects.toThrow(/invalid storage key segment/)
    await expect(storage.list([".."])).rejects.toThrow(/invalid storage key segment|escapes/)
    await expect(storage.read([])).rejects.toThrow(/must not be empty/)
  })

  test("a corrupt JSON file surfaces a typed STORAGE_PARSE error", async () => {
    await using tmp = await tmpdir()
    const root = path.join(tmp.path, "storage")
    await mkdir(root, { recursive: true })
    await writeFile(path.join(root, "bad.json"), "{not json")

    const storage = createFileStorage(root)
    const error = await storage.read(["bad"]).then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(error).toBeInstanceOf(Error)
    expect((error as { tag?: string }).tag).toBe("STORAGE_PARSE")
  })
})

describe("7.13 file-KV backend", () => {
  test("writes, reads, lists and removes JSON entries", async () => {
    await using tmp = await tmpdir()
    const storage = createFileStorage(path.join(tmp.path, "storage"))

    expect(await storage.read(["a", "b"])).toEqual({ found: false })
    await storage.write(["a", "b"], { v: 1 })
    expect(await storage.read(["a", "b"])).toEqual({ found: true, content: { v: 1 } })

    await storage.write(["a", "c"], { v: 2 })
    expect(await storage.list(["a"])).toEqual([
      ["a", "b"],
      ["a", "c"],
    ])

    await storage.remove(["a", "b"])
    expect(await storage.read(["a", "b"])).toEqual({ found: false })
    expect(await storage.list(["a"])).toEqual([["a", "c"]])

    // Removing an absent entry is idempotent (matches the Storage service).
    await storage.remove(["a", "missing"])
  })
})

describe("7.13 writer routes Storage over the port", () => {
  test("storage mutations serialize through the writer isolate", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))
    await client.openStorage(path.join(tmp.path, "storage"))

    await client.storageWrite(["session", "s1"], { id: "s1" })
    expect(await client.storageRead(["session", "s1"])).toEqual({ found: true, content: { id: "s1" } })
    expect(await client.storageList(["session"])).toEqual([["session", "s1"]])

    await client.storageRemove(["session", "s1"])
    expect(await client.storageRead(["session", "s1"])).toEqual({ found: false })
  })

  test("a storage frame without an open backend fails loud", async () => {
    await using tmp = await tmpdir()
    await using client = createWriterClient({ env: healthyEnv })
    await client.open(path.join(tmp.path, "writer.db"))

    const error = await client.storageRead(["a"]).then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toMatch(/storage is not open/)
  })
})
