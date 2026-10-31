import { describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
import fs from "node:fs/promises"
import path from "node:path"
import { MAX_HUB_PROXY_SPILL_BYTES } from "../../../src/session/worker/frame"
import {
  createHubProxySpill,
  hubProxySpillDirectoryFor,
  type HubProxySpillReference,
} from "../../../src/session/worker/hub-proxy-spill"
import { tmpdir } from "../../fixture/fixture"

const sha256 = (text: string) => createHash("sha256").update(text).digest("hex")

const rejection = async (promise: Promise<unknown>) => {
  try {
    await promise
  } catch (error) {
    return error as { _tag?: string }
  }
  throw new Error("expected rejection")
}

const reference = (hash: string, bytes: number, scope: string): HubProxySpillReference => ({
  __hubProxySpill: { hash, bytes, scope },
})

describe("hubProxySpillDirectoryFor", () => {
  test("is undefined for in-memory and missing databases", () => {
    expect(hubProxySpillDirectoryFor(undefined)).toBeUndefined()
    expect(hubProxySpillDirectoryFor("")).toBeUndefined()
    expect(hubProxySpillDirectoryFor(":memory:")).toBeUndefined()
    expect(hubProxySpillDirectoryFor("file::memory:?cache=shared")).toBeUndefined()
  })

  test("sits next to the database file", () => {
    expect(hubProxySpillDirectoryFor("/data/opencode.db")).toBe(path.join("/data", "hub-proxy-spill"))
  })
})

describe("HubProxySpill", () => {
  test("round-trips a multi-megabyte body into dir/scope/hash", async () => {
    await using tmp = await tmpdir()
    const dir = path.join(tmp.path, "spill")
    const spill = createHubProxySpill(dir)
    const body = "é".repeat(3_000_000)
    const ref = await spill.write(body, "worker-1", "test body")
    expect(ref.__hubProxySpill).toEqual({ hash: sha256(body), bytes: Buffer.byteLength(body), scope: "worker-1" })
    expect(await fs.readdir(path.join(dir, "worker-1"))).toEqual([sha256(body)])
    expect(await spill.read(ref)).toBe(body)
  })

  test("the same body twice yields one reference and one file", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    const [a, b] = await Promise.all([spill.write("same", "s", "x"), spill.write("same", "s", "x")])
    expect(a).toEqual(b)
    expect(await fs.readdir(path.join(tmp.path, "s"))).toEqual([sha256("same")])
  })

  test("read rejects a missing file", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    const error = await rejection(spill.read(reference(sha256("gone"), 4, "s")))
    expect(error._tag).toBe("HubProxySpillMissingError")
  })

  test("read rejects a size mismatch and a hash mismatch", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    const ref = await spill.write("hello", "s", "x")
    const file = path.join(tmp.path, "s", ref.__hubProxySpill.hash)
    await fs.writeFile(file, "hello world")
    const size = await rejection(spill.read(ref))
    expect(size._tag).toBe("HubProxySpillCorruptError")
    await fs.writeFile(file, "hellO")
    const hash = await rejection(spill.read(ref))
    expect(hash._tag).toBe("HubProxySpillCorruptError")
  })

  test("invalid references never touch the filesystem", async () => {
    await using tmp = await tmpdir()
    const dir = path.join(tmp.path, "spill")
    const spill = createHubProxySpill(dir)
    await rejection(spill.read(reference("nothex", 1, "s")))
    await rejection(spill.read(reference(sha256("a"), 1, "../x")))
    await rejection(spill.remove(reference(sha256("a"), 1, "../x")))
    await rejection(spill.removeScope("../x"))
    await rejection(spill.write("a", "../x", "x"))
    expect(await fs.stat(dir).catch(() => undefined)).toBeUndefined()
    expect(await fs.stat(path.join(tmp.path, "x")).catch(() => undefined)).toBeUndefined()
  })

  test("write rejects a body over the spill limit", async () => {
    await using tmp = await tmpdir()
    const dir = path.join(tmp.path, "spill")
    const spill = createHubProxySpill(dir)
    const error = (await rejection(spill.write("a".repeat(MAX_HUB_PROXY_SPILL_BYTES + 1), "s", "big body"))) as {
      _tag?: string
      bytes?: number
      limit?: number
      label?: string
    }
    expect(error._tag).toBe("HubProxySpillTooLargeError")
    expect(error.bytes).toBe(MAX_HUB_PROXY_SPILL_BYTES + 1)
    expect(error.limit).toBe(MAX_HUB_PROXY_SPILL_BYTES)
    expect(error.label).toBe("big body")
    expect(await fs.stat(dir).catch(() => undefined)).toBeUndefined()
  })

  test("remove is idempotent", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    const ref = await spill.write("body", "s", "x")
    await spill.remove(ref)
    await spill.remove(ref)
    await expect(spill.read(ref)).rejects.toMatchObject({ _tag: "HubProxySpillMissingError" })
  })

  test("removeScope removes only that scope and is idempotent", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    await spill.write("one", "a", "x")
    await spill.write("two", "a", "x")
    const kept = await spill.write("three", "b", "x")
    expect(await spill.removeScope("a")).toBe(2)
    expect(await spill.removeScope("a")).toBe(0)
    expect(await spill.read(kept)).toBe("three")
  })

  test("sweep(0) removes everything; a long age keeps fresh files", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(path.join(tmp.path, "spill"))
    const a = await spill.write("one", "a", "x")
    await spill.write("two", "b", "x")
    expect(await spill.sweep(60_000)).toBe(0)
    expect(await spill.read(a)).toBe("one")
    expect(await spill.sweep(0)).toBe(2)
    expect(await fs.readdir(path.join(tmp.path, "spill"))).toEqual([])
    expect(await spill.sweep(0)).toBe(0)
  })

  test("sweep on a missing root returns 0", async () => {
    await using tmp = await tmpdir()
    expect(await createHubProxySpill(path.join(tmp.path, "nope")).sweep(0)).toBe(0)
  })

  test("no temp files remain after writes", async () => {
    await using tmp = await tmpdir()
    const spill = createHubProxySpill(tmp.path)
    await Promise.all(["a", "b", "c"].map((body) => spill.write(body, "s", "x")))
    expect((await fs.readdir(path.join(tmp.path, "s"))).sort()).toEqual(["a", "b", "c"].map(sha256).sort())
  })
})
