// Lifecycle soak acceptance: 50 concurrent roots plus nested `task`-shaped
// children must all admit, settle, and reap back to a zero-worker baseline.
//
// The soak uses the real hub/dispatcher and in-memory executor pool with the
// existing `FakeExecutorWorker`; no provider/model is called. See
// `lifecycle-soak.ts` for the determinism rules and limitations.

import { describe, expect, test } from "bun:test"
import {
  CHILDREN_PER_ROOT,
  NESTED_EVERY_NTH_ROOT,
  ROOT_SESSIONS,
  SOAK_REAP_MS,
  runLifecycleSoak,
} from "./lifecycle-soak"

describe("session worker lifecycle soak", () => {
  test("50 roots with nested task-shaped children admit, settle, and reap to baseline", async () => {
    const metrics = await runLifecycleSoak()
    const children = ROOT_SESSIONS * CHILDREN_PER_ROOT
    const grandchildren = Math.ceil(ROOT_SESSIONS / NESTED_EVERY_NTH_ROOT)
    const sessions = ROOT_SESSIONS + children + grandchildren

    expect(metrics.roots).toBe(ROOT_SESSIONS)
    expect(metrics.children).toBe(children)
    expect(metrics.grandchildren).toBe(grandchildren)
    expect(metrics.sessions).toBe(sessions)

    // One worker per session, with task children at parent+1 and nested children
    // at parent+2.
    expect(metrics.depthCounts).toEqual({ "0": ROOT_SESSIONS, "1": children, "2": grandchildren })
    expect(metrics.peakWorkers).toBe(sessions)
    expect(metrics.queuedAtPeak).toBe(0)

    expect(metrics.settledSessions).toBe(sessions)
    expect(metrics.failedSessions).toBe(0)

    // Idle reap returns both the pool and the dispatcher registry to baseline.
    expect(metrics.reapMs).toBe(SOAK_REAP_MS)
    expect(metrics.releasedSessions).toBe(sessions)
    expect(metrics.postReapWorkers).toBe(0)
    expect(metrics.postReapRegistrySize).toBe(0)

    // No parked reap timers and no tracked worker/control-port leaks survive.
    expect(metrics.outstandingTimers).toBe(0)
    expect(metrics.leakedWorkers).toEqual([])
    expect(metrics.leakedPorts).toEqual([])

    expect(metrics.rssPeakBytes).toBeGreaterThan(0)
    expect(metrics.rssAfterReapBytes).toBeGreaterThan(0)
    expect(metrics.admissionMs).toBeGreaterThanOrEqual(0)
    expect(metrics.settleMs).toBeGreaterThanOrEqual(0)
    expect(metrics.reapWaitMs).toBeGreaterThanOrEqual(0)
    expect(metrics.totalMs).toBeGreaterThanOrEqual(metrics.reapWaitMs)

    // Optional metrics artifact for the acceptance report; normal test runs do
    // not write outside the temp/suite lifecycle.
    const artifact = process.env["SOAK_METRICS_PATH"]
    if (artifact !== undefined && artifact !== "") {
      await Bun.write(artifact, JSON.stringify(metrics, null, 2))
    }
  }, 30_000)
})
