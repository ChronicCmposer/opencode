import { afterEach, describe, expect, test } from "bun:test"
import path from "path"
import {
  abortTxAfter,
  activeFaults,
  crashWorker,
  failCommitAt,
  FAULT_KINDS,
  FAULTS_ENV,
  FaultSpecError,
  faultWorkerEntry,
  parseFaultPlan,
  serveFaults,
  withFaults,
  WORKER_CRASH_EXIT_CODE,
} from "./faults"
import { createWorkerHarness } from "./worker-harness"

/** Outbound protocol of `test/fixture/fault-worker.ts`. */
type FaultWorkerMessage =
  | { readonly type: "ready" }
  | { readonly type: "crashing"; readonly point: string }
  | { readonly type: "survived"; readonly point: string }

const originalGate = process.env[FAULTS_ENV]

afterEach(() => {
  serveFaults(originalGate)
})

describe("parseFaultPlan", () => {
  test("is disabled when the gate is unset or blank", () => {
    for (const raw of [undefined, "", "   ", ",", " , "]) {
      const plan = parseFaultPlan(raw)
      expect(plan.enabled).toBe(false)
      expect(plan.directives).toEqual([])
      expect(plan.has("crashWorker", "worker:run")).toBe(false)
    }
  })

  test("parses a single directive and scopes it to its exact point", () => {
    const plan = parseFaultPlan("crashWorker@worker:run")

    expect(plan.enabled).toBe(true)
    expect(plan.directives).toEqual([{ kind: "crashWorker", point: "worker:run" }])
    expect(plan.has("crashWorker", "worker:run")).toBe(true)
    // wrong point or wrong kind must not match
    expect(plan.has("crashWorker", "worker:other")).toBe(false)
    expect(plan.has("failCommitAt", "worker:run")).toBe(false)
  })

  test("parses multiple directives, trimming whitespace", () => {
    const plan = parseFaultPlan(" crashWorker@writer:commit , abortTxAfter@writer:tx ")

    expect(plan.enabled).toBe(true)
    expect(plan.directives).toEqual([
      { kind: "crashWorker", point: "writer:commit" },
      { kind: "abortTxAfter", point: "writer:tx" },
    ])
  })

  test("fails loudly on a malformed directive", () => {
    expect(() => parseFaultPlan("crashWorker")).toThrow(FaultSpecError)
    expect(() => parseFaultPlan("@worker:run")).toThrow(FaultSpecError)
    expect(() => parseFaultPlan("crashWorker@")).toThrow(FaultSpecError)
  })

  test("fails loudly on an unknown kind", () => {
    expect(() => parseFaultPlan("explode@worker:run")).toThrow(/unknown .* kind/)
    expect(FAULT_KINDS).toContain("crashWorker")
  })
})

describe("fault hooks are inert by default", () => {
  test("crashWorker does not invoke the crash action when unset", () => {
    serveFaults(undefined)
    let called = false
    crashWorker("worker:run", {
      onCrash: () => {
        called = true
        throw new Error("must not run")
      },
    })
    expect(called).toBe(false)
  })

  test("the commit and abort hooks report nothing when unset", () => {
    serveFaults(undefined)
    expect(failCommitAt("writer:commit")).toBeUndefined()
    expect(abortTxAfter("writer:tx")).toBe(false)
  })
})

describe("fault hooks fire at their point", () => {
  test("crashWorker fires only at the armed point", () => {
    serveFaults("crashWorker@worker:run")
    const seen: string[] = []
    const report = {
      onCrash: (directive: { kind: string; point: string }) => {
        seen.push(directive.point)
        throw new Error("stop")
      },
    }

    expect(() => crashWorker("worker:other", report)).not.toThrow()
    expect(() => crashWorker("worker:run", report)).toThrow("stop")
    expect(seen).toEqual(["worker:run"])
  })

  test("failCommitAt returns a SQLITE_BUSY fault only at the armed point", () => {
    serveFaults("failCommitAt@writer:commit")

    expect(failCommitAt("writer:other")).toBeUndefined()
    const fault = failCommitAt("writer:commit")
    expect(fault).toBeDefined()
    expect(fault?.sqliteCode).toBe("SQLITE_BUSY")
    expect(fault?.point).toBe("writer:commit")
    expect(fault?.message).toContain("SQLITE_BUSY")
  })

  test("abortTxAfter is true only at the armed point", () => {
    serveFaults("abortTxAfter@writer:tx")

    expect(abortTxAfter("writer:commit")).toBe(false)
    expect(abortTxAfter("writer:tx")).toBe(true)
  })
})

describe("activeFaults reads the gate at call time", () => {
  test("reflects a change between calls", () => {
    serveFaults(undefined)
    expect(activeFaults().enabled).toBe(false)

    serveFaults("abortTxAfter@writer:tx")
    expect(activeFaults().has("abortTxAfter", "writer:tx")).toBe(true)
  })

  test("a malformed gate fails loudly and does not poison later reads", () => {
    serveFaults("not-a-directive")
    expect(() => activeFaults()).toThrow(FaultSpecError)

    serveFaults("abortTxAfter@writer:tx")
    expect(activeFaults().has("abortTxAfter", "writer:tx")).toBe(true)
  })
})

describe("withFaults scopes the gate to a block", () => {
  test("arms inside the block and restores the prior value after", async () => {
    serveFaults("failCommitAt@writer:commit")
    const inside = await withFaults("abortTxAfter@writer:tx", () => abortTxAfter("writer:tx"))
    expect(inside).toBe(true)
    // the outer gate is back: the abort point is no longer armed...
    expect(abortTxAfter("writer:tx")).toBe(false)
    // ...and the original commit fault is armed again
    expect(failCommitAt("writer:commit")?.sqliteCode).toBe("SQLITE_BUSY")
  })

  test("restores the gate even when the block throws", async () => {
    serveFaults(undefined)
    await expect(
      withFaults("crashWorker@worker:run", () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(activeFaults().enabled).toBe(false)
  })
})

describe("the gate crosses the worker isolate boundary", () => {
  test("arms a crash inside a real worker thread", async () => {
    await using harness = createWorkerHarness({ cores: 2 })
    const worker = harness.spawn<FaultWorkerMessage>(faultWorkerEntry, {
      env: { [FAULTS_ENV]: "crashWorker@worker:run" },
    })
    await worker.received((message) => message.type === "ready")

    worker.post({ type: "crash", point: "worker:run" })
    const crashing = await worker.waitFor("crashing")
    expect(crashing.point).toBe("worker:run")

    await worker.terminate()
  })

  test("leaves the worker running when the gate is unset", async () => {
    await using harness = createWorkerHarness({ cores: 2, env: { [FAULTS_ENV]: undefined } })
    const worker = harness.spawn<FaultWorkerMessage>(faultWorkerEntry)
    await worker.received((message) => message.type === "ready")

    worker.post({ type: "crash", point: "worker:run" })
    const survived = await worker.waitFor("survived")
    expect(survived.point).toBe("worker:run")

    await worker.terminate()
  })
})

describe("crashWorker default action", () => {
  test("exits the isolate with the crash code when armed, and only then", async () => {
    const modulePath = path.join(import.meta.dir, "../../src/testing/faults.ts")
    const script = `import { Faults } from ${JSON.stringify(modulePath)}; Faults.crashWorker("worker:run")`

    const armed = Bun.spawn([process.execPath, "-e", script], {
      env: { ...process.env, [FAULTS_ENV]: "crashWorker@worker:run" },
      stdout: "ignore",
      stderr: "ignore",
    })
    expect(await armed.exited).toBe(WORKER_CRASH_EXIT_CODE)

    const unset = Bun.spawn([process.execPath, "-e", script], {
      env: { ...process.env, [FAULTS_ENV]: "" },
      stdout: "ignore",
      stderr: "ignore",
    })
    expect(await unset.exited).toBe(0)
  })
})
