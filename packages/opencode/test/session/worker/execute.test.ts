// Phase 5.1–5.4 — session execution seam (plan tasks 5.1–5.4, 5.6).
//
// The run-state machine (`Runner` per session key) is a portable unit: the hub
// runs it in-process when the rollout flag is off, the worker runs it when the
// flag is on. These tests exercise the worker-local machine and the executor
// wire protocol directly (no threads).

import { describe, expect, test } from "bun:test"
import { Effect, Exit, Scope } from "effect"
import type { SessionID } from "@/session/schema"
import {
  createExecutorWorker,
  createExecutorWorkerPool,
  createSessionExecutor,
  createSessionRunMachine,
  DEFAULT_WORK_KIND,
  installSessionExecutor,
  SessionBusyError,
  UnknownWorkKindError,
  type ExecutorTransport,
  type ExecutorWorkerScope,
  type SessionExecutionResult,
  type SessionWorkFactory,
} from "@/session/worker/execute"

const sid = (value: string) => value as SessionID

/**
 * Run `body` against a fresh Effect scope, closing it when `body` settles so the
 * machine's forked fibers never outlive the test (finding #55).
 */
const withScope = <A>(body: (scope: Scope.Scope) => Promise<A>): Promise<A> =>
  Effect.gen(function* () {
    const scope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
    return yield* Effect.promise(() => body(scope))
  }).pipe(Effect.scoped, Effect.runPromise)

/** A promise gate plus a `started` signal, both plain (no cross-runtime Effect). */
function gate() {
  let open!: () => void
  const opened = new Promise<void>((resolve) => {
    open = resolve
  })
  let started!: () => void
  const startedPromise = new Promise<void>((resolve) => {
    started = resolve
  })
  return { opened, open, startedPromise, started }
}

const result = (sessionID: string, value: unknown): SessionExecutionResult => ({ sessionID, value })

/** A work factory whose single "echo" kind returns `kind`; unknown kinds throw. */
const echoWork: SessionWorkFactory = (kind) => {
  if (kind !== "echo") throw new UnknownWorkKindError({ message: `unknown kind: ${kind}`, kind })
  return (sessionID) => Effect.succeed(result(sessionID, kind))
}

describe("5.2 worker-local run-state machine", () => {
  test("per-key serialization: concurrent ensureRunning for one key joins a single run", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      let executions = 0
      const work = Effect.gen(function* () {
        executions += 1
        yield* Effect.sleep("20 millis")
        return result("ses_1", executions)
      })

      const [a, b] = await Promise.all([
        Effect.runPromise(machine.ensureRunning(sid("ses_1"), work)),
        Effect.runPromise(machine.ensureRunning(sid("ses_1"), work)),
      ])

      expect(a).toEqual(b)
      expect(executions).toBe(1)
      expect(machine.busy(sid("ses_1"))).toBe(false)
    }))

  test("different keys run concurrently", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const g = gate()
      const work = (tag: string): Effect.Effect<SessionExecutionResult> =>
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          yield* Effect.promise(() => g.opened)
          return result(tag, tag)
        })

      const first = Effect.runPromise(machine.ensureRunning(sid("ses_a"), work("ses_a")))
      const second = Effect.runPromise(machine.ensureRunning(sid("ses_b"), work("ses_b")))
      await g.startedPromise

      expect(machine.busy(sid("ses_a"))).toBe(true)
      expect(machine.busy(sid("ses_b"))).toBe(true)

      g.open()
      const [a, b] = await Promise.all([first, second])
      expect(a.sessionID).toBe("ses_a")
      expect(b.sessionID).toBe("ses_b")
    }))

  test("assertNotBusy fails worker-local with SessionBusyError while a run is active", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const g = gate()
      const running = Effect.runPromise(
        machine.ensureRunning(
          sid("ses_1"),
          Effect.gen(function* () {
            yield* Effect.sync(() => g.started())
            yield* Effect.promise(() => g.opened)
            return result("ses_1", "done")
          }),
        ),
      )
      await g.startedPromise

      expect(machine.busy(sid("ses_1"))).toBe(true)
      await expect(Effect.runPromise(machine.assertNotBusy(sid("ses_1")))).rejects.toBeInstanceOf(SessionBusyError)

      g.open()
      await running
      expect(machine.busy(sid("ses_1"))).toBe(false)
      await Effect.runPromise(machine.assertNotBusy(sid("ses_1")))
    }))

  test("startShell rejects RunnerBusy while a run is active", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const g = gate()
      const running = Effect.runPromise(
        machine.ensureRunning(
          sid("ses_1"),
          Effect.gen(function* () {
            yield* Effect.sync(() => g.started())
            yield* Effect.promise(() => g.opened)
            return result("ses_1", "run")
          }),
        ),
      )
      await g.startedPromise

      const exit = await Effect.runPromise(
        machine.startShell(sid("ses_1"), Effect.succeed(result("ses_1", "shell"))).pipe(Effect.exit),
      )
      expect(Exit.isFailure(exit)).toBe(true)

      g.open()
      await running
      const shell = await Effect.runPromise(machine.startShell(sid("ses_1"), Effect.succeed(result("ses_1", "shell"))))
      expect(shell.value).toBe("shell")
    }))

  test("cancel interrupts a running session", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const g = gate()
      const running = Effect.runPromise(
        machine.ensureRunning(
          sid("ses_1"),
          Effect.gen(function* () {
            yield* Effect.sync(() => g.started())
            return yield* Effect.never.pipe(Effect.as(result("ses_1", "never")))
          }),
        ),
      )
      await g.startedPromise
      await Effect.runPromise(machine.cancel(sid("ses_1")))
      await expect(running).rejects.toBeDefined()
      expect(machine.busy(sid("ses_1"))).toBe(false)
    }))

  test("cancelAll cancels every active session", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const g = gate()
      const work = (tag: string): Effect.Effect<SessionExecutionResult> =>
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          return yield* Effect.never.pipe(Effect.as(result(tag, tag)))
        })
      const a = Effect.runPromise(machine.ensureRunning(sid("ses_a"), work("ses_a")))
      const b = Effect.runPromise(machine.ensureRunning(sid("ses_b"), work("ses_b")))
      await g.startedPromise

      await Effect.runPromise(machine.cancelAll)
      await expect(a).rejects.toBeDefined()
      await expect(b).rejects.toBeDefined()
      expect(machine.busy(sid("ses_a"))).toBe(false)
      expect(machine.busy(sid("ses_b"))).toBe(false)
    }))
})

describe("5.1 executor protocol", () => {
  test("ping answers pong", () =>
    withScope(async (scope) => {
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work: echoWork })
      expect(await executor.handle({ type: "ping", id: 7 })).toEqual({ type: "pong", id: 7 })
    }))

  test("run answers done with the work result", () =>
    withScope(async (scope) => {
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work: echoWork })
      const out = await executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "echo" })
      expect(out).toEqual({ type: "done", id: 2, result: result("ses_1", "echo") })
    }))

  test("an unknown kind answers an error carrying the tag", () =>
    withScope(async (scope) => {
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work: echoWork })
      const out = await executor.handle({ type: "run", id: 3, sessionID: sid("ses_1"), kind: "missing" })
      expect(out.type).toBe("error")
      if (out.type === "error") expect(out.tag).toBe("UnknownWorkKindError")
    }))

  test("assertNotBusy reflects worker-local state", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const executor = createSessionExecutor({ machine, work: echoWork })
      expect(await executor.handle({ type: "assertNotBusy", id: 1, sessionID: sid("ses_1") })).toEqual({
        type: "busy",
        id: 1,
        busy: false,
      })

      const g = gate()
      const slowWork: SessionWorkFactory = () => (sessionID) =>
        Effect.gen(function* () {
          yield* Effect.sync(() => g.started())
          yield* Effect.promise(() => g.opened)
          return result(sessionID, "slow")
        })
      const slow = createSessionExecutor({ machine, work: slowWork })
      const pending = slow.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "slow" })
      await g.startedPromise

      expect(await slow.handle({ type: "assertNotBusy", id: 3, sessionID: sid("ses_1") })).toEqual({
        type: "busy",
        id: 3,
        busy: true,
      })

      g.open()
      expect((await pending).type).toBe("done")
    }))

  test("two runs for one key share a single execution", () =>
    withScope(async (scope) => {
      let executions = 0
      const work: SessionWorkFactory = () => (sessionID) =>
        Effect.gen(function* () {
          executions += 1
          yield* Effect.sleep("20 millis")
          return result(sessionID, executions)
        })
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work })

      const [a, b] = await Promise.all([
        executor.handle({ type: "run", id: 1, sessionID: sid("ses_1"), kind: "shared" }),
        executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "shared" }),
      ])

      expect(a.type).toBe("done")
      expect(b.type).toBe("done")
      if (a.type === "done" && b.type === "done") expect(a.result).toEqual(b.result)
      expect(executions).toBe(1)
    }))

  test("cancel answers cancelled", () =>
    withScope(async (scope) => {
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work: echoWork })
      expect(await executor.handle({ type: "cancel", id: 9, sessionID: sid("ses_1") })).toEqual({
        type: "cancelled",
        id: 9,
      })
    }))

  test("DEFAULT_WORK_KIND is the production placeholder kind", () => {
    expect(DEFAULT_WORK_KIND).toBe("noop")
  })
})

describe("5.7 executor robustness", () => {
  test("a defect in the work yields a structured error frame", () =>
    withScope(async (scope) => {
      const defectWork: SessionWorkFactory = () => () => Effect.die(new Error("defect boom"))
      const executor = createSessionExecutor({
        machine: createSessionRunMachine({ scope }),
        work: defectWork,
      })
      const out = await executor.handle({ type: "run", id: 5, sessionID: sid("ses_1"), kind: "defect" })
      expect(out.type).toBe("error")
      if (out.type === "error") expect(out.message).toContain("defect boom")
    }))

  test("installSessionExecutor exposes a dispose that closes its owned scope", async () => {
    const posted: unknown[] = []
    const workerScope: ExecutorWorkerScope = { onmessage: null, postMessage: (message) => posted.push(message) }
    const installed = installSessionExecutor(workerScope, { work: echoWork })
    expect(posted[0]).toEqual({ type: "ready", threadId: 0 })
    await expect(installed.dispose()).resolves.toBeUndefined()
  })

  test("extra executor workers share one monotonic id and drop from the pool on terminate", async () => {
    const spawnIds: number[] = []
    const transport = (): ExecutorTransport => {
      let alive = true
      return {
        post: () => {},
        waitFor: async () => ({ type: "pong", id: 0 }),
        get alive() {
          return alive
        },
        async terminate() {
          alive = false
          return 0
        },
      }
    }
    const pool = createExecutorWorkerPool({
      k: 2,
      spawn: (id) => {
        spawnIds.push(id)
        return createExecutorWorker({ id, transport: transport() })
      },
    })
    expect(spawnIds).toEqual([1, 2])

    const first = pool.spawn("headroom")
    const second = pool.spawn("headroom")
    expect(spawnIds).toEqual([1, 2, 3, 4])
    expect(pool.byID(first.id)).toBeDefined()

    await first.terminate()
    expect(pool.byID(first.id)).toBeUndefined()

    const third = pool.spawn("headroom")
    expect(third.id).toBe(5)
    await pool.terminateAll()
  })
})
