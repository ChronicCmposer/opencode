// Structural guard: `session/db` is not a barrel.
//
// Each sibling module is imported by its own path and re-exports its own
// namespace (`export * as X from "./x"`), so ownership is visible at every call
// site instead of hidden behind a package-level bucket.

import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"

const packageRoot = path.join(import.meta.dir, "../../..")
const srcRoot = path.join(packageRoot, "src")
const testRoot = path.join(packageRoot, "test")
const dbRoot = path.join(srcRoot, "session/db")

const SIBLINGS = ["allowlist.gen", "derive", "ownership", "schema"] as const

function sourceFiles(root: string): string[] {
  const entries = fs.readdirSync(root, { withFileTypes: true })
  return entries.flatMap((entry) => {
    const entryPath = path.join(root, entry.name)
    if (entry.isDirectory()) return sourceFiles(entryPath)
    if (!/\.(ts|tsx|mts|cts)$/.test(entry.name)) return []
    return [entryPath]
  })
}

function importsBarrel(text: string): boolean {
  return /(?:from|import)\s*\(?\s*["']@\/session\/db(?:\/index)?["']/.test(text)
}

describe("session/db import boundary", () => {
  test("the barrel is gone", () => {
    expect(fs.existsSync(path.join(dbRoot, "index.ts"))).toBe(false)
  })

  test("no source or test imports the barrel", () => {
    const offenders = [...sourceFiles(srcRoot), ...sourceFiles(testRoot)].flatMap((file) => {
      const text = fs.readFileSync(file, "utf8")
      return importsBarrel(text) ? [path.relative(packageRoot, file)] : []
    })
    expect(offenders).toEqual([])
  })

  test("every sibling exposes its own namespace", () => {
    const missing = SIBLINGS.flatMap((name) => {
      const text = fs.readFileSync(path.join(dbRoot, `${name}.ts`), "utf8")
      const pattern = new RegExp(`export \\* as [A-Za-z_$][\\w$]* from "\\./${name.replace(".", "\\.")}"`)
      return pattern.test(text) ? [] : [name]
    })
    expect(missing).toEqual([])
  })
})
