// D16 blob durability: a content-addressed write must reach stable storage
// BEFORE the atomic rename makes it visible, and the directory entry must be
// durable AFTER the rename. Without those two barriers a crash (or power loss)
// can leave a committed `event_blob` row pointing at a zero-length or missing
// file, so the hub cannot resolve a durable event by position.
//
// The write still dedupes by content hash, still writes through a temp file,
// and still removes that temp file on any failure.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, spyOn, test } from "bun:test"
import { DurableLog } from "@opencode-ai/core/durable-log"

const directories: string[] = []
const restores: Array<() => void> = []

afterEach(() => {
  while (restores.length > 0) restores.pop()?.()
  while (directories.length > 0) fs.rmSync(directories.pop()!, { recursive: true, force: true })
})

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-blob-fsync-"))
  directories.push(directory)
  return directory
}

interface Trace {
  readonly events: string[]
  readonly openFlags: string[]
  failFileSync?: boolean
}

/**
 * Record the durability barriers the production writer actually performs. A
 * file fsync is distinguished from a directory fsync by `fstat`, so a test can
 * pin the order across the rename instead of only counting calls. The open flags
 * are recorded too: a file must be opened writable (`r+`) before fsync, while a
 * directory handle can only be opened read-only.
 */
function traceDurability(): Trace {
  const trace: Trace = { events: [], openFlags: [] }
  const originalFsync = fs.fsyncSync
  const originalRename = fs.renameSync
  const originalOpen = fs.openSync
  const open = spyOn(fs, "openSync").mockImplementation(((target: fs.PathLike, flags: string, mode?: number) => {
    trace.openFlags.push(flags)
    return originalOpen(target, flags as never, mode as never)
  }) as typeof fs.openSync)
  restores.push(() => open.mockRestore())
  const fsync = spyOn(fs, "fsyncSync").mockImplementation(((fd: number) => {
    if (trace.failFileSync === true) throw new Error("fsync failed")
    trace.events.push(fs.fstatSync(fd).isDirectory() ? "fsync-dir" : "fsync-file")
    return originalFsync(fd)
  }) as typeof fs.fsyncSync)
  restores.push(() => fsync.mockRestore())
  const rename = spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
    trace.events.push("rename")
    originalRename(from, to)
  }) as typeof fs.renameSync)
  restores.push(() => rename.mockRestore())
  return trace
}

describe("blob-fsync: durable content-addressed writes", () => {
  test("fsyncs the file before the rename and the directory after it", () => {
    const directory = workspace()
    const payload = JSON.stringify({ text: "x".repeat(1024) })
    const hash = DurableLog.digest(payload)
    const trace = traceDurability()

    DurableLog.writeBlobFile(directory, hash, payload)

    expect(trace.events).toEqual(["fsync-file", "rename", "fsync-dir"])
    // The file data must be flushed through a writable descriptor; only the
    // directory entry can use a read-only handle.
    expect(trace.openFlags).toEqual(["r+", "r"])
    expect(fs.readFileSync(path.join(directory, hash), "utf8")).toBe(payload)
    expect(fs.readdirSync(directory)).toEqual([hash])
  })

  test("dedupes an already-durable blob without rewriting but still re-syncs it", () => {
    const directory = workspace()
    const payload = JSON.stringify({ text: "dedupe" })
    const hash = DurableLog.digest(payload)
    DurableLog.writeBlobFile(directory, hash, payload)

    const trace = traceDurability()
    DurableLog.writeBlobFile(directory, hash, payload)

    // The fast path must not rewrite or rename the existing file, but a
    // committed blob row still requires the file and its directory entry to be
    // durable on this call.
    expect(trace.events).toEqual(["fsync-file", "fsync-dir"])
    expect(trace.openFlags).toEqual(["r+", "r"])
  })

  test("propagates a failed fsync and leaves no temp file behind", () => {
    const directory = workspace()
    const payload = JSON.stringify({ text: "failure" })
    const hash = DurableLog.digest(payload)
    const trace = traceDurability()
    trace.failFileSync = true

    // A failed data flush must fail the write and leave no temp file.
    expect(() => DurableLog.writeBlobFile(directory, hash, payload)).toThrow("fsync failed")
    expect(fs.readdirSync(directory)).toEqual([])
  })
})
