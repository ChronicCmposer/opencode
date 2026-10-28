import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"

const packageRoot = path.join(import.meta.dir, "../../..")
const srcRoot = path.join(packageRoot, "src")
const testRoot = path.join(packageRoot, "test")

const REVIEW_ROUND_NAME =
  /(?:^|[-_.])(?:p[0-2](?:fix)?|lowfix|review-fix|final(?:[-_.][a-z0-9]+)*|wave[0-9]+|hardening)(?=[-_.]|$)/i

function sourceFiles(root: string): string[] {
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(root, entry.name)
    if (entry.isDirectory()) return sourceFiles(entryPath)
    if (!/\.(ts|tsx|mts|cts)$/.test(entry.name)) return []
    return [entryPath]
  })
}

describe("idiom guard", () => {
  test("the repository barrel stays gone", () => {
    expect(fs.existsSync(path.join(srcRoot, "session/worker/repository/index.ts"))).toBe(false)
    const imports = sourceFiles(srcRoot).filter((file) =>
      /(?:from|import)\s*\(?\s*["']@\/session\/worker\/repository(?:\/index)?["']/.test(
        fs.readFileSync(file, "utf8"),
      ),
    )
    expect(imports).toEqual([])
  })

  test("worker repository does not wrap a fresh typed error in Effect.fail", () => {
    const offenders = sourceFiles(path.join(srcRoot, "session/worker/repository"))
      .filter((file) => /Effect\.fail\(\s*new\s/.test(fs.readFileSync(file, "utf8")))
      .map((file) => path.relative(packageRoot, file))
    expect(offenders).toEqual([])
  })

  test("no review-round test filenames remain", () => {
    const offenders = sourceFiles(testRoot)
      .map((file) => path.relative(testRoot, file))
      .filter((relative) => REVIEW_ROUND_NAME.test(relative))
    expect(offenders).toEqual([])
  })

  test("EventV2 worker state is not held in module globals", () => {
    const source = fs.readFileSync(path.join(packageRoot, "../core/src/event.ts"), "utf8")
    expect(source).not.toMatch(/^let worker(?:LeaseOwner|SessionScope|DurableOperations)/m)
  })
})
