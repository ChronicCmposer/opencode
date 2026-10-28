// The test-only egress hub and fan-out had no production
// caller (~340 lines of production-dead code). They now live under `test/` and
// back the hub/SSE stream tests only. These assertions are the grep guard that
// keeps them from silently drifting back into `src/`.

import { describe, expect, test } from "bun:test"
import { existsSync, readdirSync, readFileSync } from "node:fs"
import { join } from "node:path"

const SRC = join(import.meta.dir, "..", "..", "..", "src")

/** Every `.ts` file under `dir`, recursively. */
function typescriptFiles(dir: string): string[] {
  const files: string[] = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) files.push(...typescriptFiles(path))
    else if (entry.name.endsWith(".ts")) files.push(path)
  }
  return files
}

describe("egress production boundary", () => {
  test("the unused hub/fanout live under test/, not src/", () => {
    expect(existsSync(join(SRC, "session/egress/hub.ts"))).toBe(false)
    expect(existsSync(join(SRC, "session/egress/fanout.ts"))).toBe(false)
  })

  test("no production module imports the relocated hub/fanout", () => {
    const offenders = typescriptFiles(SRC).filter((file) => {
      const source = readFileSync(file, "utf8")
      return source.includes("session/egress/hub") || source.includes("session/egress/fanout")
    })
    expect(offenders).toEqual([])
  })
})
