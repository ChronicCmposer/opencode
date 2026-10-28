import { describe, expect, test } from "bun:test"
import { compareIds } from "../../src/util/order"
import { search } from "../../src/context/sync"

type Part = { id: string; text: string }

const byId = (part: Part, target: string) => compareIds(part.id, target)

/** Server payload order: SQLite `ORDER BY id` (binary collation). */
const SQL_ORDER = [
  "prt_019bcf2a1000Xy7QmNp3Rt",
  "prt_019bcf2a19f0Xy7QmNp3Rt",
  "prt_019bcf2a19f1Xy7QmNp3Rt",
  "prt_019bcf2a1a00Xy7QmNp3Rt",
  "prt_019bcf2a1a1fXy7QmNp3Rt",
]

function hydrate(): Part[] {
  return SQL_ORDER.map((id) => ({ id, text: "" }))
}

/** Mirrors `message.part.updated`: replace when found, otherwise insert at the search index. */
function apply(parts: Part[], part: Part): "inserted" | "replaced" {
  const match = search(parts, part.id, byId)
  if (match.found) {
    parts[match.index] = part
    return "replaced"
  }
  parts.splice(match.index, 0, part)
  return "inserted"
}

describe("sync id binary search", () => {
  test("finds every part the server returned in SQL id order", () => {
    const parts = hydrate()
    for (const [index, id] of SQL_ORDER.entries()) {
      const match = search(parts, id, byId)
      expect(match.found).toBe(true)
      expect(match.index).toBe(index)
    }
  })

  test("replaces existing parts instead of duplicating them", () => {
    const parts = hydrate()
    const updates = [SQL_ORDER[0]!, SQL_ORDER[2]!, SQL_ORDER[4]!, SQL_ORDER[0]!]

    for (const id of updates) {
      expect(apply(parts, { id, text: id })).toBe("replaced")
    }

    expect(parts.map((part) => part.id)).toEqual(SQL_ORDER)
    expect(parts[0]!.text).toBe(SQL_ORDER[0]!)
  })

  test("inserts a new part at the SQL-order position without duplicating on retry", () => {
    const parts = hydrate()
    const created = "prt_019bcf2a19f2Xy7QmNp3Rt"

    expect(apply(parts, { id: created, text: created })).toBe("inserted")
    expect(apply(parts, { id: created, text: created })).toBe("replaced")

    expect(parts.map((part) => part.id)).toEqual([
      "prt_019bcf2a1000Xy7QmNp3Rt",
      "prt_019bcf2a19f0Xy7QmNp3Rt",
      "prt_019bcf2a19f1Xy7QmNp3Rt",
      created,
      "prt_019bcf2a1a00Xy7QmNp3Rt",
      "prt_019bcf2a1a1fXy7QmNp3Rt",
    ])
  })
})
