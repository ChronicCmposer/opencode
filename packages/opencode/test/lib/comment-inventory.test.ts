import { describe, expect, test } from "bun:test"
import { inventoryByPackage, inventoryCommentReferences, packageOf } from "./comment-inventory"

describe("comment purge inventory", () => {
  test("records parallel-implement and plan references from comments only", () => {
    const references = inventoryCommentReferences([
      {
        path: "packages/opencode/src/session/worker/nodes.ts",
        content: [
          "// Applied wiring (Wave 3):",
          'const label = "Step 5 Preview Free"',
          "// see .parallel-implement/PLAN-V2.md",
          "/* D18 barrier note */",
          "const x = 1",
        ].join("\n"),
      },
    ])

    expect(references.map((entry) => entry.kind)).toEqual(["plan-reference", "parallel-implement"])
    expect(references.map((entry) => entry.line)).toEqual([1, 3])
    expect(references[0]?.column).toBe(20)
  })

  test("ignores code strings that mention plan-looking text", () => {
    const references = inventoryCommentReferences([
      { path: "packages/console/app/src/i18n/en.ts", content: '"go.promo.step5": "Step 5 Preview Free",' },
    ])
    expect(references).toEqual([])
  })

  test("groups references by package deterministically", () => {
    const references = inventoryCommentReferences([
      { path: "packages/opencode/src/b.ts", content: "// W2.5 barrier" },
      { path: "packages/core/src/a.ts", content: "// W1.7 bus" },
      { path: "packages/opencode/src/a.ts", content: "// .parallel-implement/x.md" },
    ])
    const grouped = inventoryByPackage(references)
    expect(grouped.map((entry) => entry.package)).toEqual(["packages/core", "packages/opencode"])
    expect(grouped[1]?.count).toBe(2)
    expect(grouped[1]?.references.map((entry) => entry.path)).toEqual([
      "packages/opencode/src/a.ts",
      "packages/opencode/src/b.ts",
    ])
  })

  test("packageOf names the owning package or root", () => {
    expect(packageOf("packages/opencode/src/a.ts")).toBe("packages/opencode")
    expect(packageOf("script/build.ts")).toBe("(root)")
  })
})
