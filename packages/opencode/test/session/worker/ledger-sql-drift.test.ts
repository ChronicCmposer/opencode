// The ledger SQL has exactly one source of truth.
//
// `sql/001-tool-call.sql` is the hand-written schema; `sql/fallback.gen.ts` is
// emitted from it by `script/migration-statements.ts` for the compiled binary,
// which has no adjacent `.sql` file. The fallback must equal the source byte for
// byte, and `tool-call.ts` must not carry a third inline copy.

import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import { LEDGER_SQL_FALLBACK } from "@/session/ledger/sql/fallback.gen"
import { classifyWorkerStatement } from "@/session/worker/db"

const sqlPath = path.join(import.meta.dir, "../../../src/session/ledger/sql/001-tool-call.sql")
const sourcePath = path.join(import.meta.dir, "../../../src/session/ledger/tool-call.ts")

describe("ledger SQL fallback", () => {
  test("the generated fallback equals the SQL source byte for byte", () => {
    expect(LEDGER_SQL_FALLBACK).toBe(fs.readFileSync(sqlPath, "utf8"))
  })

  test("tool-call.ts consumes the generated fallback instead of inlining it", () => {
    const source = fs.readFileSync(sourcePath, "utf8")
    expect(source).toContain('from "./sql/fallback.gen"')
    expect(source).not.toContain("CREATE TABLE IF NOT EXISTS tool_call")
  })
})

describe("worker statement keyword classifier", () => {
  const workerSource = path.join(import.meta.dir, "../../../src/session/worker")

  test("one leading-keyword implementation is shared by the router and the fence", () => {
    const db = fs.readFileSync(path.join(workerSource, "db.ts"), "utf8")
    const write = fs.readFileSync(path.join(workerSource, "repository/write.ts"), "utf8")
    expect(write).toContain("export function leadingKeyword")
    expect(db).toContain('import { leadingKeyword, makeRepository } from "./repository/write"')
    expect(db).not.toMatch(/function leadingKeyword\s*\(/)
  })

  test("comments and parenthesized prefixes classify to the same keyword", () => {
    expect(classifyWorkerStatement("/* c */ -- x\n  INSERT INTO t VALUES (1)").keyword).toBe("insert")
    expect(classifyWorkerStatement("(select 1)").keyword).toBe("select")
    // A comment after the opening parenthesis must not defeat the classifier.
    expect(classifyWorkerStatement("( /* c */ BEGIN )").keyword).toBe("begin")
    expect(classifyWorkerStatement("( /* c */ select 1)").keyword).toBe("select")
  })
})
