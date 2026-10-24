// Phase 7.13 — the writer's file-KV backend (plan task 7.13).
//
// `Storage` in this repo is file-KV (`<dir>/<key...>.json`), not SQLite. This
// adapter gives the writer core a filesystem implementation so the writer can
// serialize storage mutations on its single queue; the session worker proxies
// here over the storage frames in `./protocol` instead of touching the disk.
//
// Paths mirror `src/storage/storage.ts` (`path.join(dir, ...key) + ".json"`), so
// the writer and the direct-FS path address the same files.

import { randomUUID } from "node:crypto"
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import { WriterError, type StorageReadResult, type WriterStorage } from "./core"
import { isEnoent } from "./protocol"

const JSON_SUFFIX = ".json"

/**
 * Serialize file-KV content. `JSON.stringify(undefined)` yields `undefined`
 * (not a string), which `writeFile` rejects; persist an explicit `null` instead
 * so a stored `undefined` is a defined value rather than a failed write.
 */
function encode(content: unknown): string {
  return JSON.stringify(content, null, 2) ?? "null"
}

/**
 * Write `text` to `file` atomically: a crash between truncate and write would
 * otherwise leave a partial/corrupt JSON file. The value is fully written to a
 * sibling temp file (deliberately not `*.json`, so it is never read or listed)
 * and only then renamed over the target.
 */
async function writeAtomic(file: string, text: string): Promise<void> {
  const temp = `${file}.${randomUUID()}.tmp`
  try {
    await writeFile(temp, text)
    await rename(temp, file)
  } catch (error) {
    await rm(temp, { force: true })
    throw error
  }
}

/**
 * Resolve `segments` under `dir`, rejecting any segment that could escape the
 * storage root (`.` / `..` / separators / absolute paths). A key is data, never a
 * path fragment, so this is a parse failure, not a validation to recover from.
 */
function resolveWithin(dir: string, segments: ReadonlyArray<string>): string {
  for (const segment of segments) {
    if (
      segment === "" ||
      segment === "." ||
      segment === ".." ||
      segment.includes("/") ||
      segment.includes("\\") ||
      path.isAbsolute(segment)
    ) {
      throw new WriterError("STORAGE_PATH", `invalid storage key segment ${JSON.stringify(segment)}`)
    }
  }
  const base = path.resolve(dir)
  const resolved = path.resolve(base, ...segments)
  if (resolved !== base && !resolved.startsWith(base + path.sep)) {
    throw new WriterError("STORAGE_PATH", `storage key escapes the storage root: ${JSON.stringify([...segments])}`)
  }
  return resolved
}

function target(dir: string, key: ReadonlyArray<string>): string {
  if (key.length === 0) throw new WriterError("STORAGE_PATH", "storage key must not be empty")
  return resolveWithin(dir, key) + JSON_SUFFIX
}

function parseJson(text: string, file: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    throw new WriterError("STORAGE_PARSE", `stored JSON at ${file} is not valid JSON`)
  }
}

export function createFileStorage(dir: string): WriterStorage {
  return {
    async read(key): Promise<StorageReadResult> {
      const file = target(dir, key)
      let text: string
      try {
        text = await readFile(file, "utf8")
      } catch (error) {
        if (isEnoent(error)) return { found: false }
        throw error
      }
      return { found: true, content: parseJson(text, file) }
    },
    async write(key, content): Promise<void> {
      const file = target(dir, key)
      await mkdir(path.dirname(file), { recursive: true })
      await writeAtomic(file, encode(content))
    },
    async update(key, fn): Promise<StorageReadResult> {
      const file = target(dir, key)
      let text: string
      try {
        text = await readFile(file, "utf8")
      } catch (error) {
        if (isEnoent(error)) return { found: false }
        throw error
      }
      const content = parseJson(text, file)
      fn(content)
      await writeAtomic(file, encode(content))
      return { found: true, content }
    },
    async remove(key): Promise<void> {
      try {
        await rm(target(dir, key))
      } catch (error) {
        if (!isEnoent(error)) throw error
      }
    },
    async list(prefix): Promise<ReadonlyArray<ReadonlyArray<string>>> {
      const root = resolveWithin(dir, prefix)
      const out: string[][] = []
      const walk = async (current: string, parts: string[]): Promise<void> => {
        let entries
        try {
          entries = await readdir(current, { withFileTypes: true })
        } catch (error) {
          if (isEnoent(error)) return
          throw error
        }
        for (const entry of entries) {
          if (entry.isDirectory()) await walk(path.join(current, entry.name), [...parts, entry.name])
          else if (entry.name.endsWith(JSON_SUFFIX)) out.push([...prefix, ...parts, entry.name.slice(0, -JSON_SUFFIX.length)])
        }
      }
      await walk(root, [])
      return out.toSorted((a, b) => a.join("/").localeCompare(b.join("/")))
    },
  }
}

export * as WriterStorageModule from "./storage"
