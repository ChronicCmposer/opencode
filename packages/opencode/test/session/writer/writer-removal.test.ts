// Source-graph guard: the writer isolate, its RPC/port wrappers and the
// write-through cache must not come back. Only `connection.ts` (the per-worker
// WAL helper) may remain under `src/session/writer`.

import { describe, expect, test } from "bun:test"
import { readdirSync, readFileSync } from "node:fs"
import path from "node:path"

const PACKAGE_ROOT = path.resolve(import.meta.dir, "../../..")
const SRC = path.join(PACKAGE_ROOT, "src")
const WRITER_DIR = path.join(SRC, "session/writer")

/**
 * Consumers that still import a deleted writer module. Each entry is removal
 * debt owned by another agent; the guard exists so no *new* consumer appears
 * and so this list cannot quietly grow. When the owning agent lands, delete its
 * entry here.
 */
const PENDING_CONSUMERS: Readonly<Record<string, string>> = {}

/** Scripts that still mention a deleted writer entry (none after the writer removal). */
const PENDING_SCRIPT_CONSUMERS: Readonly<Record<string, string>> = {}

const DELETED_MODULE_IMPORT =
  /from\s+["'][^"']*(?:session\/writer\/(?:worker|protocol|core|client|storage)|writer-(?:sql-)?client)["']/
const DELETED_SYMBOL = /\b(?:WriterCore|WriteThroughCache)\b/
const DELETED_WRITER_REFERENCE =
  /session\/writer\/(?:worker|protocol|core|client|storage)|writer-(?:sql-)?client|writerWorker|OPENCODE_WRITER_WORKER_PATH/

function sourceFiles(dir: string): string[] {
  const out: string[] = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) out.push(...sourceFiles(full))
    else if (entry.name.endsWith(".ts")) out.push(full)
  }
  return out
}

const relative = (file: string) => path.relative(SRC, file).split(path.sep).join("/")

const TEST_ROOT = path.join(PACKAGE_ROOT, "test")
const relativeTest = (file: string) => path.relative(TEST_ROOT, file).split(path.sep).join("/")
const SELF = "session/writer/writer-removal.test.ts"

describe("writer removal (source graph)", () => {
  test("only the connection helper and the generated snapshot remain", () => {
    expect(readdirSync(WRITER_DIR).sort()).toEqual(["connection.ts", "migration.statements.gen.ts"])
  })

  test("no new consumer imports a deleted writer module", () => {
    const offenders = sourceFiles(SRC)
      .filter((file) => relative(file) !== "session/writer/connection.ts")
      .filter((file) => DELETED_MODULE_IMPORT.test(readFileSync(file, "utf8")))
      .map(relative)
      .filter((file) => PENDING_CONSUMERS[file] === undefined)
    expect(offenders).toEqual([])
  })

  test("every remaining import is a known pending consumer", () => {
    const actual = sourceFiles(SRC)
      .filter((file) => relative(file) !== "session/writer/connection.ts")
      .filter((file) => DELETED_MODULE_IMPORT.test(readFileSync(file, "utf8")))
      .map(relative)
    for (const file of actual) expect(PENDING_CONSUMERS[file]).toBeDefined()
  })

  test("no deleted writer symbol is referenced anywhere in src", () => {
    const offenders = sourceFiles(SRC)
      .filter((file) => relative(file) !== "session/writer/connection.ts")
      .filter((file) => DELETED_SYMBOL.test(readFileSync(file, "utf8")))
      .map(relative)
    expect(offenders).toEqual([])
  })

  test("no test imports or mocks the deleted writer RPC", () => {
    const offenders = sourceFiles(TEST_ROOT)
      .filter((file) => relativeTest(file) !== SELF)
      .filter((file) => {
        const source = readFileSync(file, "utf8")
        return DELETED_MODULE_IMPORT.test(source) || DELETED_SYMBOL.test(source)
      })
      .map(relativeTest)
    expect(offenders).toEqual([])
  })

  test("no unowned script references the deleted writer entry", () => {
    const offenders = sourceFiles(path.join(PACKAGE_ROOT, "script"))
      .filter((file) => DELETED_WRITER_REFERENCE.test(readFileSync(file, "utf8")))
      .map((file) => path.relative(PACKAGE_ROOT, file).split(path.sep).join("/"))
      .filter((file) => PENDING_SCRIPT_CONSUMERS[file] === undefined)
    expect(offenders).toEqual([])
  })
})
