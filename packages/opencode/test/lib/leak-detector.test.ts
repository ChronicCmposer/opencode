// Plan task 3.9: leak detector for workers, MessagePorts and child PGIDs.
//
// Unit cases drive the detector with injected probes so the *detection logic*
// is deterministic; integration cases reuse `worker-harness` for workers and
// `process-tree` for a real POSIX process group. The child-PGID case is
// skipped on Windows with a documented reason.

import { describe, expect, test } from "bun:test"
import { MessageChannel } from "node:worker_threads"
import path from "node:path"
import { createLeakDetector } from "./leak-detector"
import { killProcessGroup, posixSupport, processGroupId, spawnDetached, waitForDeath } from "./process-tree"
import { createWorkerHarness, type WorkerMessage } from "./worker-harness"

const workerEntry = path.join(import.meta.dir, "../fixture/harness-worker.ts")
const fixture = path.join(import.meta.dir, "../fixture/mcp-lsp-descendants.ts")
const support = posixSupport()

describe("leak detector", () => {
  test("reports a clean environment when nothing is tracked", () => {
    const detector = createLeakDetector({ posix: true })
    const report = detector.detect()

    expect(report.clean).toBe(true)
    expect(report.leaks).toEqual([])
    expect(report.workers).toEqual([])
    expect(report.ports).toEqual([])
    expect(report.processGroups).toEqual([])
  })

  test("reports leaked workers, ports and process groups via the injected probes", () => {
    const detector = createLeakDetector({ posix: true, isProcessGroupAlive: () => true })
    detector.trackWorker({ id: 7, alive: true })
    const channel = new MessageChannel()
    detector.trackPort(channel.port1, "control")
    detector.trackProcessGroup(4242)
    channel.port2.close()

    const report = detector.detect()

    expect(report.clean).toBe(false)
    expect(report.workers).toEqual([7])
    expect(report.ports).toEqual(["control"])
    expect(report.processGroups).toEqual([4242])
    expect(report.leaks.map((leak) => leak.kind)).toEqual(["worker", "port", "process-group"])
    expect(report.leaks.every((leak) => leak.detail.length > 0)).toBe(true)
  })

  test("ignores released ports, dead workers and dead process groups", () => {
    let groupAlive = true
    const detector = createLeakDetector({ posix: true, isProcessGroupAlive: () => groupAlive })
    detector.trackWorker({ id: 1, alive: false })
    const channel = new MessageChannel()
    const port = detector.trackPort(channel.port1)
    detector.trackProcessGroup(99)

    port.close()
    channel.port2.close()
    groupAlive = false

    expect(detector.detect().clean).toBe(true)
    expect(port.closed).toBe(true)
  })

  test("fails loudly on an invalid tracked process group", () => {
    const detector = createLeakDetector({ posix: true, isProcessGroupAlive: () => true })

    expect(() => detector.trackProcessGroup(0)).toThrow(/process group/)
    expect(() => detector.trackProcessGroup(-4)).toThrow(/process group/)
  })

  test("returns an immutable snapshot", () => {
    const detector = createLeakDetector({ posix: true, isProcessGroupAlive: () => true })
    detector.trackWorker({ id: 3, alive: true })

    const report = detector.detect()
    expect(Object.isFrozen(report)).toBe(true)
    expect(Object.isFrozen(report.leaks)).toBe(true)
    expect(Object.isFrozen(report.leaks[0])).toBe(true)
    expect(() => {
      ;(report.leaks[0] as { detail: string }).detail = "mutated"
    }).toThrow()
  })

  test("reuses the worker-harness registry to detect a leaked worker", async () => {
    await using harness = createWorkerHarness({ cores: 1 })
    const detector = createLeakDetector({ posix: true, workers: () => harness.workers })

    const worker = harness.spawn<WorkerMessage>(workerEntry)
    await worker.received((message) => message.type === "ready")
    expect(detector.detect().workers).toEqual([worker.id])

    await harness.terminateAll()
    expect(detector.detect().clean).toBe(true)
  })

  test.skipIf(!support.supported)(
    "detects a leaked child process group and turns clean after a group kill",
    async () => {
      const child = spawnDetached([process.execPath, fixture], {
        stdin: "ignore",
        stdout: "ignore",
        env: { CHILD_TREE_KIND: "mcp", CHILD_TREE_FANOUT: "2", CHILD_TREE_DEPTH: "1" },
      })
      const detector = createLeakDetector()
      detector.trackProcessGroup(child.pgid)

      try {
        expect(detector.detect().processGroups).toEqual([child.pgid])

        killProcessGroup(child.pgid, "SIGTERM")
        expect(await waitForDeath([child.pid], { timeoutMs: 10_000 })).toBe(true)
        expect(detector.detect().clean).toBe(true)
      } finally {
        killProcessGroup(child.pgid, "SIGKILL")
        await child.exited
      }
    },
    20_000,
  )

  test.skipIf(support.supported)("child PGID detection is skipped on non-POSIX platforms", () => {
    // Documents the reason the POSIX cases are skipped rather than silently absent.
    expect(support.reason).toMatch(/POSIX/)
  })

  test.skipIf(!support.supported)("default POSIX probe delegates to process-tree isGroupAlive", () => {
    const pgid = processGroupId(process.pid)!
    const detector = createLeakDetector()
    detector.trackProcessGroup(pgid)
    expect(detector.detect().processGroups).toEqual([pgid])
  })
})
