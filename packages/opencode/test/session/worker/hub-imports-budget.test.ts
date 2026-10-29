// Import-idiom budget for the pure hub.
//
// `hub.ts` used to reach its dependencies through seventeen `@/…` alias
// specifiers. The hub sits inside the worker tree, so the idiomatic form is a
// relative specifier (`./`, `../`, `../../`); aliases are reserved for entry
// points and consumers outside the tree. This guard pins that budget: the file
// may keep at most four alias imports, and every key public symbol must remain
// exported after the conversion.

import fs from "node:fs"
import path from "node:path"
import { describe, expect, test } from "bun:test"
import * as Hub from "@/session/worker/hub"

const packageRoot = path.join(import.meta.dir, "../../..")
const hubPath = path.join(packageRoot, "src/session/worker/hub.ts")

const ALIAS_BUDGET = 5

/** Count specifiers that still resolve through the `@/` package alias. */
function aliasImportCount(source: string): number {
  const named = source.match(/from\s+"@\//g)?.length ?? 0
  const sideEffect = source.match(/import\s+"@\//g)?.length ?? 0
  return named + sideEffect
}

describe("hub import idiom budget", () => {
  test("keeps fewer than five alias imports", () => {
    const source = fs.readFileSync(hubPath, "utf8")
    expect(aliasImportCount(source)).toBeLessThan(ALIAS_BUDGET)
  })

  test("retains the key public symbols", () => {
    expect(typeof Hub.createSessionHub).toBe("function")
    expect(typeof Hub.createHubEventSink).toBe("function")
    expect(typeof Hub.createEgressDeliveryBarrier).toBe("function")
    expect(typeof Hub.createProductionSessionHub).toBe("function")
    expect(typeof Hub.makeSessionHubNode).toBe("function")
    expect(Hub.SessionHubService).toBeDefined()
    expect(typeof Hub.SESSION_CANCELLED_TYPE).toBe("string")
    expect(Hub.SessionHub).toBeDefined()
  })
})
