// fix2-ws3 — the explicit per-isolate process role.
//
// The role replaces the ambient `isMainThread` check that used to gate the
// session-hub node's graph membership. Module state is per-isolate: the process
// main thread is the `hub`, and a session-worker isolate sets its own role at
// boot. Because Bun shares module state across test files in one run, the
// set-once semantics are exercised inside a dedicated worker isolate (the
// fixture) so the shared main-process role is never mutated.

import { describe, expect, test } from "bun:test"
import { processRole } from "@/effect/process-role"
import { createWorkerHarness } from "../../lib/worker-harness"

const PROCESS_ROLE_FIXTURE = new URL("../../fixture/process-role-worker.ts", import.meta.url)

interface RoleReport {
  readonly type: "result"
  readonly defaultRole: string | undefined
  readonly nodesBefore: number
  readonly roleAfter: string
  readonly nodesAfter: number
  readonly conflict: string | null
  readonly sameRole: string | null
}

describe("processRole", () => {
  test("never assumes hub authority (fail closed when unset)", () => {
    // The main test process declares `hub` via `project/bootstrap.ts`, so the
    // explicit role is observable; an undeclared role is covered in the fresh
    // isolate below, where the graph must contribute nothing.
    const role = processRole()
    expect(role === undefined || role === "hub").toBe(true)
  })

  test("is set-once per isolate and gates sessionWorkerNodes on 'hub'", async () => {
    const harness = createWorkerHarness({ timeoutMs: 15_000 })
    try {
      const worker = harness.spawn<RoleReport>(PROCESS_ROLE_FIXTURE)
      const report = await worker.waitFor("result")

      // Fresh isolate: no role is assumed, so the hub gate stays closed.
      expect(report.defaultRole).toBeUndefined()
      expect(report.nodesBefore).toBe(0)

      // After the single set, the role and the graph decision follow.
      expect(report.roleAfter).toBe("session")
      expect(report.nodesAfter).toBe(0)

      // A conflicting re-set fails loud; the same role is an idempotent no-op.
      if (report.conflict === null) throw new Error("expected the conflicting re-set to fail loudly")
      expect(report.conflict).toContain("session")
      expect(report.conflict).toContain("writer")
      expect(report.sameRole).toBeNull()
    } finally {
      await harness.dispose()
    }
  })
})
