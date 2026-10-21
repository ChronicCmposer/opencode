// Phase 7.13 — the writer serializes file-KV `Storage` (plan task 7.13).
//
// `Storage` is file-KV, not SQLite, so it needs a non-SQL mutation family on the
// writer. These tests cover the fs backend directly and the REAL worker isolate
// over the storage frames (openStorage / storage.read|write|remove|list).

import { describe, expect, test } from "bun:test"
import path from "node:path"
import { createWriterClient } from "@/session/writer/client"
import { createFileStorage } from "@/session/writer/storage"
import { FAULTS_ENV } from "@/testing/faults"
import { tmpdir } from "../../fixture/fixture"

/** A client whose writer isolate never sees an armed fault. */
const healthyEnv = { [FAULTS_ENV]: undefined }

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
