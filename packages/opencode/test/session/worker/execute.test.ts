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
  RESULTS_CAP,
  SessionBusyError,
  SessionWorkerReplyTimeoutError,
  UnknownWorkKindError,
  type ExecutorTransport,
  type ExecutorWorker,
  type ExecutorWorkerScope,
  type SessionExecutionResult,
  type SessionExecutorInbound,
  type SessionExecutorOutbound,
  type SessionRunControl,
  type SessionWorkFactory,
} from "@/session/worker/execute"
import type { PromptInput } from "@/session/prompt"
import type { CancelFrame } from "@/session/worker/cancel"
import type { WorkerLogger } from "@/session/worker/log"

const sid = (value: string) => value as SessionID

/** A recording logger: captures every `(message, fields)` pair in call order. */
function recordingLog(): {
  readonly log: WorkerLogger
  readonly entries: Array<{ message: string; fields: Record<string, unknown> }>
} {
  const entries: Array<{ message: string; fields: Record<string, unknown> }> = []
  return { log: (message, fields) => entries.push({ message, fields }), entries }
}

/** A transport that answers nothing and can be terminated; enough to build a worker. */
const noopTransport = (): ExecutorTransport => {
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

  test("DEFAULT_WORK_KIND is the production session work kind", () => {
    expect(DEFAULT_WORK_KIND).toBe("session")
  })
})

describe("WS3 onRunStart run-start seam", () => {
  test("onRunStart fires once with the run sessionID before the work executes", () =>
    withScope(async (scope) => {
      const calls: SessionID[] = []
      const order: string[] = []
      const work: SessionWorkFactory = () => (sessionID) =>
        Effect.sync(() => {
          order.push("work")
          return result(sessionID, "ran")
        })
      const executor = createSessionExecutor({
        machine: createSessionRunMachine({ scope }),
        work,
        onRunStart: (sessionID) => {
          calls.push(sessionID)
          order.push("onRunStart")
        },
      })

      const out = await executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "echo" })

      expect(out).toEqual({ type: "done", id: 2, result: result("ses_1", "ran") })
      expect(calls).toEqual([sid("ses_1")])
      expect(order).toEqual(["onRunStart", "work"])
    }))

  test("a throwing onRunStart never fails the run", () =>
    withScope(async (scope) => {
      const executor = createSessionExecutor({
        machine: createSessionRunMachine({ scope }),
        work: echoWork,
        onRunStart: () => {
          throw new Error("thread rename boom")
        },
      })

      const out = await executor.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "echo" })

      expect(out).toEqual({ type: "done", id: 2, result: result("ses_1", "echo") })
    }))

  test("installSessionExecutor forwards onRunStart to the executor", async () => {
    const workerScope: ExecutorWorkerScope = { onmessage: null, postMessage: () => {} }
    const calls: SessionID[] = []
    const installed = installSessionExecutor(workerScope, {
      work: echoWork,
      onRunStart: (sessionID) => calls.push(sessionID),
    })

    await installed.handle({ type: "run", id: 2, sessionID: sid("ses_1"), kind: "echo" })
    expect(calls).toEqual([sid("ses_1")])
    await installed.dispose()
  })
})

describe("5.4 SessionRunControl seam (Phase 4 + 6.3)", () => {
  test("cancel routes to the provided run control, not the executor machine", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const cancelled: SessionID[] = []
      const control: SessionRunControl = {
        cancel: async (sessionID) => {
          cancelled.push(sessionID)
        },
        busy: async () => false,
      }
      const executor = createSessionExecutor({ machine, work: echoWork, control })

      expect(await executor.handle({ type: "cancel", id: 9, sessionID: sid("ses_1") })).toEqual({
        type: "cancelled",
        id: 9,
      })
      expect(cancelled).toEqual([sid("ses_1")])
    }))

  test("assertNotBusy answers from the provided run control", () =>
    withScope(async (scope) => {
      const machine = createSessionRunMachine({ scope })
      const executor = createSessionExecutor({
        machine,
        work: echoWork,
        control: { cancel: async () => {}, busy: async () => true },
      })

      expect(await executor.handle({ type: "assertNotBusy", id: 1, sessionID: sid("ses_1") })).toEqual({
        type: "busy",
        id: 1,
        busy: true,
      })
    }))

  test("installSessionExecutor forwards the run control to the executor", async () => {
    const posted: unknown[] = []
    const workerScope: ExecutorWorkerScope = { onmessage: null, postMessage: (message) => posted.push(message) }
    const installed = installSessionExecutor(workerScope, {
      work: echoWork,
      control: { cancel: async () => {}, busy: async () => true },
    })

    expect(await installed.handle({ type: "assertNotBusy", id: 1, sessionID: sid("ses_1") })).toEqual({
      type: "busy",
      id: 1,
      busy: true,
    })
    await installed.dispose()
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

describe("13 executor cancel port", () => {
  /** A transport whose `waitFor` replies to the most recent request. */
  const replyingTransport = (
    reply: (request: unknown) => unknown,
  ): { readonly transport: ExecutorTransport; readonly posted: { readonly type: string }[] } => {
    const posted: { readonly type: string }[] = []
    let alive = true
    return {
      posted,
      transport: {
        post: (message) => posted.push(message as { readonly type: string }),
        waitFor: async (match) => {
          const outbound = reply(posted[posted.length - 1] ?? {}) as never
          if (match(outbound)) return outbound
          throw new Error("no matching reply")
        },
        get alive() {
          return alive
        },
        async terminate() {
          alive = false
          return 0
        },
      },
    }
  }

  test("a cancel frame drives the worker cancel and acks acceptance", async () => {
    const { transport, posted } = replyingTransport((request) => ({
      type: "cancelled",
      id: (request as { readonly id?: number }).id,
    }))
    const worker = createExecutorWorker({ id: 1, transport })
    const acks: CancelFrame[] = []
    worker.cancelPort?.onMessage((frame) => acks.push(frame))

    worker.cancelPort?.notify({ type: "cancel", sessionID: "ses_1" })
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(posted.some((message) => message.type === "cancel")).toBe(true)
    expect(acks).toEqual([{ type: "cancel-ack", sessionID: "ses_1", accepted: true }])
  })

  test("a failed cancel acks with accepted: false", async () => {
    const transport: ExecutorTransport = {
      post: () => {},
      waitFor: async () => {
        throw new Error("cancel transport failed")
      },
      alive: true,
      async terminate() {
        return 0
      },
    }
    const worker = createExecutorWorker({ id: 1, transport })
    const acks: CancelFrame[] = []
    worker.cancelPort?.onMessage((frame) => acks.push(frame))

    worker.cancelPort?.notify({ type: "cancel", sessionID: "ses_1" })
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(acks).toEqual([{ type: "cancel-ack", sessionID: "ses_1", accepted: false }])
  })

  test("D2 an accepted cancel emits 'cancel outcome' as cooperative", async () => {
    const { transport } = replyingTransport((request) => ({
      type: "cancelled",
      id: (request as { readonly id?: number }).id,
    }))
    const { log, entries } = recordingLog()
    const worker = createExecutorWorker({ id: 1, transport, log })

    worker.cancelPort?.notify({ type: "cancel", sessionID: "ses_1" })
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(entries).toContainEqual({
      message: "session workers: cancel outcome",
      fields: { sessionID: "ses_1", mode: "cooperative", accepted: true },
    })
  })

  test("D2 a rejected cancel emits 'cancel outcome' as terminate", async () => {
    const transport: ExecutorTransport = {
      post: () => {},
      waitFor: async () => {
        throw new Error("cancel transport failed")
      },
      alive: true,
      async terminate() {
        return 0
      },
    }
    const { log, entries } = recordingLog()
    const worker = createExecutorWorker({ id: 1, transport, log })

    worker.cancelPort?.notify({ type: "cancel", sessionID: "ses_1" })
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(entries).toContainEqual({
      message: "session workers: cancel outcome",
      fields: { sessionID: "ses_1", mode: "terminate", accepted: false },
    })
  })
})

describe("12 B1 worker spawning logging", () => {
  test("createExecutorWorker emits 'worker spawning' with the default reason", () => {
    const { log, entries } = recordingLog()
    createExecutorWorker({ id: 7, transport: noopTransport(), workKind: "session", log })

    expect(entries).toContainEqual({
      message: "session workers: worker spawning",
      fields: { workerID: 7, kind: "session", reason: "initial" },
    })
  })

  test("createExecutorWorker includes sessionID when known and honors an explicit reason", () => {
    const { log, entries } = recordingLog()
    createExecutorWorker({
      id: 3,
      transport: noopTransport(),
      workKind: "session",
      sessionID: sid("ses_1"),
      reason: "rebind",
      log,
    })

    expect(entries).toContainEqual({
      message: "session workers: worker spawning",
      fields: { workerID: 3, kind: "session", sessionID: "ses_1", reason: "rebind" },
    })
  })
})

describe("liveness watchdog (execute)", () => {
  /**
   * A transport whose worker answers every `ping` with a `pong` and only
   * completes a `run` once it has answered `pongsBeforeDone` pings. Frames are
   * matched by predicate exactly like the real transport, so the run waiter and
   * a pong waiter coexist without stealing each other's frames.
   */
  const pongingTransport = (
    pongsBeforeDone: number,
  ): { readonly transport: ExecutorTransport; readonly pings: () => number } => {
    let pings = 0
    let run: { readonly id: number; readonly sessionID: SessionID } | undefined
    const waiters = new Set<{
      readonly predicate: (message: SessionExecutorOutbound) => boolean
      readonly resolve: (message: SessionExecutorOutbound) => void
    }>()
    const deliver = (message: SessionExecutorOutbound): void => {
      for (const waiter of [...waiters]) {
        if (!waiter.predicate(message)) continue
        waiters.delete(waiter)
        waiter.resolve(message)
      }
    }
    const answerPing = (id: number): void => {
      pings += 1
      queueMicrotask(() => deliver({ type: "pong", id }))
      const pending = run
      if (pending !== undefined && pings >= pongsBeforeDone) {
        run = undefined
        queueMicrotask(() => deliver({ type: "done", id: pending.id, result: { sessionID: pending.sessionID } }))
      }
    }
    return {
      pings: () => pings,
      transport: {
        post: (message) => {
          if (message.type === "ping") answerPing(message.id)
          else if (message.type === "run" || message.type === "startShell") {
            run = { id: message.id, sessionID: message.sessionID }
          }
        },
        waitFor: (predicate, options) =>
          new Promise<SessionExecutorOutbound>((resolve, reject) => {
            const waitMs = options?.timeoutMs ?? 30_000
            let timer: ReturnType<typeof setTimeout> | undefined
            const waiter = {
              predicate,
              resolve: (message: SessionExecutorOutbound) => {
                if (timer !== undefined) clearTimeout(timer)
                waiters.delete(waiter)
                resolve(message)
              },
            }
            if (waitMs > 0) {
              timer = setTimeout(() => {
                waiters.delete(waiter)
                reject(new SessionWorkerReplyTimeoutError(waitMs))
              }, waitMs)
            }
            waiters.add(waiter)
          }),
        alive: true,
        terminate: async () => 0,
      },
    }
  }

  test("a run survives several idle intervals when the worker answers pings", async () => {
    const { transport, pings } = pongingTransport(2)
    const worker = createExecutorWorker({ id: 1, transport, workKind: "slow", runIdleTimeoutMs: 15 })

    await worker.execute({ sessionID: sid("ses_alive"), depth: 0 })

    expect(pings()).toBeGreaterThanOrEqual(2)
    expect(worker.resultFor(sid("ses_alive"))?.sessionID).toBe("ses_alive")
  })

  test("a worker that stops answering pings times the run out", async () => {
    const transport: ExecutorTransport = {
      post: () => {},
      waitFor: (_match, options) => {
        const waitMs = options?.timeoutMs ?? 30_000
        if (waitMs <= 0) return new Promise(() => {})
        return new Promise((_resolve, reject) =>
          setTimeout(() => reject(new SessionWorkerReplyTimeoutError(waitMs)), waitMs),
        )
      },
      alive: true,
      terminate: async () => 0,
    }
    const worker = createExecutorWorker({ id: 1, transport, workKind: "slow", runIdleTimeoutMs: 15 })

    await expect(worker.execute({ sessionID: sid("ses_wedged"), depth: 0 })).rejects.toBeInstanceOf(
      SessionWorkerReplyTimeoutError,
    )
  })
})

// ---------------------------------------------------------------------------
// B2 — the sub-agent prompt crosses the executor frame. A dispatched child runs
// on a headroom worker whose `SessionWork` only knew how to `loop`; the prompt
// the parent would have persisted is carried on the run frame so the worker can
// persist + loop instead of reading a message that does not exist yet.
// ---------------------------------------------------------------------------

describe("B2 run prompt transport", () => {
  /**
   * A transport that records every posted frame and answers the most recent
   * `run`/`startShell` with a `done` reply, so `execute` settles. The reply is
   * matched by the executor's own predicate, mirroring the real transport.
   */
  const capturingTransport = (): {
    readonly transport: ExecutorTransport
    readonly posted: SessionExecutorInbound[]
  } => {
    const posted: SessionExecutorInbound[] = []
    let alive = true
    return {
      posted,
      transport: {
        post: (message) => posted.push(message),
        waitFor: async (match) => {
          const last = posted[posted.length - 1] as { readonly id: number; readonly sessionID: SessionID }
          const reply: SessionExecutorOutbound = { type: "done", id: last.id, result: { sessionID: last.sessionID } }
          if (match(reply)) return reply
          throw new Error("no matching reply")
        },
        get alive() {
          return alive
        },
        async terminate() {
          alive = false
          return 0
        },
      },
    }
  }

  const carriedPrompt: PromptInput = {
    sessionID: sid("ses_1"),
    agent: "build",
    parts: [{ type: "text", text: "do the thing" }],
  }

  test("a run carrying a prompt forwards it on the run frame", async () => {
    const { transport, posted } = capturingTransport()
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_1"), depth: 0, prompt: carriedPrompt })

    const run = posted.find((message) => message.type === "run")
    expect(run).toBeDefined()
    if (run?.type === "run") expect(run.prompt).toEqual(carriedPrompt)
  })

  test("a prompt-less run forwards a frame without a prompt (unchanged shape)", async () => {
    const { transport, posted } = capturingTransport()
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_2"), depth: 0 })

    const run = posted.find((message) => message.type === "run")
    expect(run).toBeDefined()
    if (run !== undefined) expect("prompt" in run).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// Shell dispatch (Part 1), result-map bounding (A9), pool cancel port (A10) and
// the durable-egress write barrier (2.6 / R4#16).
// ---------------------------------------------------------------------------

/** A transport that answers the most recent run/startShell with a distinct result. */
function replyingTransport(): { readonly transport: ExecutorTransport; readonly posted: SessionExecutorInbound[] } {
  const posted: SessionExecutorInbound[] = []
  let alive = true
  let n = 0
  return {
    posted,
    transport: {
      post: (message) => posted.push(message),
      waitFor: async (match) => {
        const last = posted[posted.length - 1] as { readonly id: number; readonly sessionID: SessionID }
        n += 1
        const reply: SessionExecutorOutbound = { type: "done", id: last.id, result: { sessionID: last.sessionID, value: n } }
        if (match(reply)) return reply
        throw new Error("no matching reply")
      },
      get alive() {
        return alive
      },
      async terminate() {
        alive = false
        return 0
      },
    },
  }
}

describe("Part 1 shell dispatch over the executor frame", () => {
  const shell = { sessionID: sid("ses_1"), agent: "build", command: "ls" }

  test("a startShell frame carries the shell command to the work", () =>
    withScope(async (scope) => {
      const seen: Array<{ prompt: unknown; shell: unknown }> = []
      const work: SessionWorkFactory = () => (sessionID, prompt, passedShell) =>
        Effect.sync(() => {
          seen.push({ prompt, shell: passedShell })
          return result(sessionID, "shell")
        })
      const executor = createSessionExecutor({ machine: createSessionRunMachine({ scope }), work })

      const out = await executor.handle({ type: "startShell", id: 1, sessionID: sid("ses_1"), kind: "x", shell })

      expect(out).toEqual({ type: "done", id: 1, result: result("ses_1", "shell") })
      expect(seen).toEqual([{ prompt: undefined, shell }])
    }))

  test("createExecutorWorker posts the shell command on the startShell frame", async () => {
    const { transport, posted } = replyingTransport()
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_1"), depth: 0, mode: "startShell", shell })

    const frame = posted.find((message) => message.type === "startShell")
    expect(frame).toBeDefined()
    if (frame?.type === "startShell") expect(frame.shell).toEqual(shell)
  })

  test("a shell-less startShell frame omits the shell field", async () => {
    const { transport, posted } = replyingTransport()
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    await worker.execute({ sessionID: sid("ses_1"), depth: 0, mode: "startShell" })

    const frame = posted.find((message) => message.type === "startShell")
    expect(frame).toBeDefined()
    if (frame !== undefined) expect("shell" in frame).toBe(false)
  })
})

describe("A9 the per-session results map is bounded", () => {
  test("re-dispatch refreshes recency and the oldest result is evicted at the cap", async () => {
    const { transport } = replyingTransport()
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session" })

    for (let index = 0; index < RESULTS_CAP; index++) {
      await worker.execute({ sessionID: sid(`ses_${index}`), depth: 0 })
    }
    // Refresh ses_0 so it is no longer the oldest.
    await worker.execute({ sessionID: sid("ses_0"), depth: 0 })
    // One more distinct session pushes the map past the cap.
    await worker.execute({ sessionID: sid("ses_extra"), depth: 0 })

    expect(worker.resultFor(sid("ses_0"))).toBeDefined()
    expect(worker.resultFor(sid("ses_extra"))).toBeDefined()
    // ses_1 was the oldest live entry and is the eviction victim.
    expect(worker.resultFor(sid("ses_1"))).toBeUndefined()
  })
})

describe("A10 the pool spawn wrapper exposes the cancel port", () => {
  test("a spawned pool worker carries its cancel port", async () => {
    const pool = createExecutorWorkerPool({
      k: 1,
      spawn: (id) => createExecutorWorker({ id, transport: noopTransport() }),
    })

    const spawned = pool.spawn("headroom") as ExecutorWorker
    expect(spawned.cancelPort).toBeDefined()
    await pool.terminateAll()
  })
})

describe("2.6 durable-egress write barrier", () => {
  test("execute does not resolve or record the result until egress is acked", async () => {
    const { transport } = replyingTransport()
    let releaseBarrier!: () => void
    const barrier = new Promise<void>((resolve) => {
      releaseBarrier = resolve
    })
    const worker = createExecutorWorker({ id: 1, transport, workKind: "session", egressBarrier: () => barrier })

    let settled = false
    const running = worker.execute({ sessionID: sid("ses_1"), depth: 0 }).then(() => {
      settled = true
    })
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(settled).toBe(false)
    expect(worker.resultFor(sid("ses_1"))).toBeUndefined()

    releaseBarrier()
    await running
    expect(settled).toBe(true)
    expect(worker.resultFor(sid("ses_1"))).toBeDefined()
  })
})
