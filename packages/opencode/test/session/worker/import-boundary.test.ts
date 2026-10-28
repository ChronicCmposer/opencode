import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"

const packageRoot = path.join(import.meta.dir, "../../..")
const srcRoot = path.join(packageRoot, "src")
const testRoot = path.join(packageRoot, "test")

interface Boundary {
  readonly name: string
  readonly directory: string
  readonly importPattern: RegExp
  readonly siblings: readonly string[]
}

const BOUNDARIES: readonly Boundary[] = [
  {
    name: "session/db",
    directory: path.join(srcRoot, "session/db"),
    importPattern: /(?:from|import)\s*\(?\s*["']@\/session\/db(?:\/index)?["']/,
    siblings: ["allowlist.gen", "derive", "ownership", "schema"],
  },
  {
    name: "session/worker/repository",
    directory: path.join(srcRoot, "session/worker/repository"),
    importPattern: /(?:from|import)\s*\(?\s*["']@\/session\/worker\/repository(?:\/index)?["']/,
    siblings: ["capability", "durable", "errors", "policy", "registry", "tables.gen", "types", "write"],
  },
]

function sourceFiles(root: string): string[] {
  return fs.readdirSync(root, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(root, entry.name)
    if (entry.isDirectory()) return sourceFiles(entryPath)
    if (!/\.(ts|tsx|mts|cts)$/.test(entry.name)) return []
    return [entryPath]
  })
}

for (const boundary of BOUNDARIES) {
  describe(`${boundary.name} import boundary`, () => {
    test("the barrel is gone", () => {
      expect(fs.existsSync(path.join(boundary.directory, "index.ts"))).toBe(false)
    })

    test("no source or test imports the barrel", () => {
      const offenders = [...sourceFiles(srcRoot), ...sourceFiles(testRoot)].flatMap((file) => {
        const text = fs.readFileSync(file, "utf8")
        return boundary.importPattern.test(text) ? [path.relative(packageRoot, file)] : []
      })
      expect(offenders).toEqual([])
    })

    test("every hand-written sibling exposes its own namespace", () => {
      const missing = boundary.siblings.flatMap((name) => {
        if (name === "tables.gen") return []
        const text = fs.readFileSync(path.join(boundary.directory, `${name}.ts`), "utf8")
        const escaped = name.replace(".", "\\.")
        const pattern = new RegExp(`export \\* as [A-Za-z_$][\\w$]* from "\\./${escaped}"`)
        return pattern.test(text) ? [] : [name]
      })
      expect(missing).toEqual([])
    })
  })
}
