// Comment purge (D24): the opencode package source no longer names the build
// plan, and the retired `writer` / `broker` process roles are gone from the
// node table and the role guard.

import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { ROLES, isRole } from "@/effect/process-role"
import { SESSION_ALLOWLIST, SESSION_EXCLUDED } from "@/session/worker/node-table"
import { inventoryCommentReferences } from "../../lib/comment-inventory"

const srcRoot = path.join(import.meta.dir, "../../../src")

function sourceFiles(root: string): string[] {
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(root, entry.name)
    if (entry.isDirectory()) return sourceFiles(entryPath)
    return /\.(ts|tsx|mts|cts)$/.test(entry.name) ? [entryPath] : []
  })
}

const RETIRED_ROLES = ["writer", "broker"]

describe("comment purge", () => {
  test("the opencode source carries no plan references", () => {
    const files = sourceFiles(srcRoot).map((file) => ({
      path: path.relative(srcRoot, file),
      content: fs.readFileSync(file, "utf8"),
    }))
    expect(inventoryCommentReferences(files)).toEqual([])
  })

  test("the retired process roles are unrepresentable", () => {
    expect([...ROLES]).toEqual(["session", "hub"])
    for (const role of RETIRED_ROLES) expect(isRole(role)).toBe(false)
  })

  test("the node table names only live roles", () => {
    const roles = [...SESSION_ALLOWLIST, ...SESSION_EXCLUDED].flatMap((node) => node.roles)
    expect(roles.filter((role) => RETIRED_ROLES.includes(role))).toEqual([])
  })
})
