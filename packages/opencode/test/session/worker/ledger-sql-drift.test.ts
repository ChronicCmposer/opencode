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
