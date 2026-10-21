// Phase 7.13 — the writer's file-KV backend (plan task 7.13).
//
// `Storage` in this repo is file-KV (`<dir>/<key...>.json`), not SQLite. This
// adapter gives the writer core a filesystem implementation so the writer can
// serialize storage mutations on its single queue; the session worker proxies
// here over the storage frames in `./protocol` instead of touching the disk.
//
// Paths mirror `src/storage/storage.ts` (`path.join(dir, ...key) + ".json"`), so
// the writer and the direct-FS path address the same files.

import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "node:path"
import type { StorageReadResult, WriterStorage } from "./core"

function target(dir: string, key: ReadonlyArray<string>): string {
  return path.join(dir, ...key) + ".json"
}

function isMissing(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as { code: unknown }).code === "ENOENT"
}

export function createFileStorage(dir: string): WriterStorage {
  return {
    async read(key): Promise<StorageReadResult> {
      try {
        const text = await readFile(target(dir, key), "utf8")
        return { found: true, content: JSON.parse(text) }
      } catch (error) {
        if (isMissing(error)) return { found: false }
        throw error
      }
    },
    async write(key, content): Promise<void> {
      const file = target(dir, key)
      await mkdir(path.dirname(file), { recursive: true })
      await writeFile(file, JSON.stringify(content, null, 2))
    },
    async remove(key): Promise<void> {
      try {
        await rm(target(dir, key))
      } catch (error) {
        if (!isMissing(error)) throw error
      }
    },
    async list(prefix): Promise<ReadonlyArray<ReadonlyArray<string>>> {
      const root = path.join(dir, ...prefix)
      const out: string[][] = []
      const walk = async (current: string, parts: string[]): Promise<void> => {
        let entries
        try {
          entries = await readdir(current, { withFileTypes: true })
        } catch (error) {
          if (isMissing(error)) return
          throw error
        }
        for (const entry of entries) {
          if (entry.isDirectory()) await walk(path.join(current, entry.name), [...parts, entry.name])
          else if (entry.name.endsWith(".json")) out.push([...prefix, ...parts, entry.name.slice(0, -5)])
        }
      }
      await walk(root, [])
      return out.toSorted((a, b) => a.join("/").localeCompare(b.join("/")))
    },
  }
}

export * as WriterStorageModule from "./storage"
