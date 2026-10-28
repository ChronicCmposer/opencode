// A `WITH` statement is classified by its top-level statement, not its first
// keyword. The statement router treats `ins/update/delete/replace` as writes and
// routes them to `main` under the authorizer bracket; a CTE read must stay on the
// scoped view. Misclassifying `WITH … INSERT` as `with` silently bypasses that
// route, so the classifier proves the DML after the CTE body wins.

import { describe, expect, test } from "bun:test"
import { classifyWorkerStatement } from "@/session/worker/db"

const keyword = (sql: string) => classifyWorkerStatement(sql).keyword

describe("CTE statement classification", () => {
  test("a top-level DML after the CTE body is a write", () => {
    expect(keyword("WITH cte AS (SELECT 1) INSERT INTO message (id) VALUES ('m')")).toBe("insert")
    expect(keyword("WITH cte AS (SELECT 1) UPDATE message SET data = '{}'")).toBe("update")
    expect(keyword("WITH cte AS (SELECT 1) DELETE FROM message")).toBe("delete")
    expect(keyword("WITH cte AS (SELECT 1) REPLACE INTO message (id) VALUES ('m')")).toBe("replace")
    expect(keyword("WITH cte AS (SELECT 1) INSERT INTO message SELECT * FROM cte")).toBe("insert")
  })

  test("a CTE read stays a read on the scoped view", () => {
    expect(keyword("WITH cte AS (SELECT 1) SELECT * FROM cte")).toBe("select")
    expect(
      keyword("WITH RECURSIVE cte (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM cte WHERE n < 3) SELECT * FROM cte"),
    ).toBe("select")
  })

  test("multiple CTEs and quoted/comment content do not confuse the main keyword", () => {
    expect(keyword("WITH a AS (SELECT '(' AS x), b AS (SELECT ')' AS y) SELECT * FROM a, b")).toBe("select")
    expect(keyword('WITH "update" AS (SELECT 1) SELECT * FROM "update"')).toBe("select")
    expect(keyword("WITH a AS (/* insert into nope */ SELECT 1) INSERT INTO message (id) VALUES ('m')")).toBe("insert")
  })

  test("a malformed WITH stays conservative", () => {
    expect(keyword("WITH")).toBe("with")
    expect(keyword("WITH cte AS (SELECT 1")).toBe("with")
  })
})
