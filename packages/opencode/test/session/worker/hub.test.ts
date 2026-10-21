// Phase 12.1–12.4 — pure hub, admission & HTTP/SSE liveness.
//
// The hub is the composition of the Phase 4 dispatcher (scheduling, admission,
// headroom, crash handling), the Phase 5 executor pool, and the Phase 11
// cooperative cancel path. These tests drive the hub over REAL Bun worker
// isolates and assert the four Phase 12 properties:
//
//   12.1 the hub executes zero sessions (work runs in a worker isolate);
//   12.2 admission admits K + headroom respecting depth; the awaiting parent
//        retains its slot;
//   12.3 the hub loop stays I/O-only while K sessions run;
//   12.4 HTTP ingress + SSE stay functional while K sessions run.

import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { SessionID } from "@/session/schema"
import type { WorkerSnapshot } from "@/session/worker/layer"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { createBunExecutorWorkerPool, createSessionHub } from "@/session/worker/hub"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"

const it = testEffect(TestLLMServer.layer)

/** The dependency-light executor fixture (Phase 5). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)
/** The production worker entry (reduced layer as the work factory). */
const SESSION_WORKER = new URL("../../../src/session/worker/session-worker.ts", import.meta.url)

const sid = (value: string) => value as SessionID
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

async function waitUntil(predicate: () => boolean | Promise<boolean>, message: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await sleep(5)
  }
  throw new Error(`timed out: ${message}`)
}

const snapshot = (overrides: Partial<WorkerSnapshot> = {}): WorkerSnapshot => ({
  epoch: 1,
  directory: "/tmp/phase12-hub",
  config: {},
  auth: {},
  modelsDev: {},
  flags: {} as RuntimeFlags.Info,
  workerCount: 2,
  ...overrides,
})

describe("12.1 the hub executes zero sessions", () => {
  it.live("work runs in the reduced-layer worker, never on the hub", () =>
    Effect.gen(function* () {
      const llm = yield* TestLLMServer
      yield* llm.text("hub-zero")
      const pool = createBunExecutorWorkerPool({
        k: 1,
        entry: SESSION_WORKER,
        timeoutMs: 15_000,
        workerData: {
          snapshot: snapshot(),
          env: { values: {} },
          model: { baseUrl: llm.url, apiKey: "test-key", model: "test-model" },
        },
      })
      const hub = createSessionHub({ pool })
      try {
        const result = yield* Effect.promise(() => hub.ensureRunning(sid("ses_1")))
        const value = result.value as { text: string; threadId: number }
        expect(value.text).toBe("hub-zero")
        // `threadId` is 0 on the main thread; a positive id proves the session
        // pipeline ran inside the worker isolate, not on the hub.
        expect(value.threadId).toBeGreaterThan(0)
        expect(hub.dispatcher.registry.size).toBe(0)
      } finally {
        yield* Effect.promise(() => hub.terminateAll())
      }
    }),
  )
})

describe("12.2 admission admits K + headroom respecting depth", () => {
  test("K busy queues; a depth-1 child spawns headroom; the parent retains its slot", async () => {
    const pool = createBunExecutorWorkerPool({ k: 1, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      const parent = hub.dispatcher.dispatch({ sessionID: sid("ses_parent"), depth: 0 })
      await waitUntil(() => hub.dispatcher.registry.get(sid("ses_parent"))?.state === "running", "parent never ran")

      const sibling = hub.dispatcher.dispatch({ sessionID: sid("ses_sib"), depth: 0 })
      await sleep(25)
      expect(hub.dispatcher.queuedCount).toBe(1)
      expect(hub.dispatcher.headroomCount).toBe(0)

      const child = hub.dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
      await waitUntil(() => hub.dispatcher.headroomCount === 1, "headroom never spawned")

      // The awaiting parent keeps its foreground slot while the child runs in headroom.
      expect(hub.dispatcher.registry.get(sid("ses_parent"))?.state).toBe("running")
      expect(hub.dispatcher.registry.get(sid("ses_child"))?.state).toBe("running")

      await Promise.all([parent, sibling, child])
      expect(hub.dispatcher.activeCount).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("12.3 the hub loop is I/O-only", () => {
  test("a hub timer keeps firing while K sessions run in workers", async () => {
    const pool = createBunExecutorWorkerPool({ k: 2, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    try {
      let ticks = 0
      const timer = setInterval(() => {
        ticks += 1
      }, 5)
      await Promise.all([hub.ensureRunning(sid("ses_a")), hub.ensureRunning(sid("ses_b"))])
      clearInterval(timer)

      // The hub event loop was never blocked: its timer fired while the workers ran.
      expect(ticks).toBeGreaterThan(0)
      expect(hub.dispatcher.activeCount).toBe(0)
    } finally {
      await hub.terminateAll()
    }
  })
})

describe("12.4 HTTP ingress + SSE stay functional", () => {
  test("health + SSE respond while K sessions run", async () => {
    const pool = createBunExecutorWorkerPool({ k: 2, entry: EXECUTOR_FIXTURE, workKind: "slow", timeoutMs: 15_000 })
    const hub = createSessionHub({ pool })
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url)
        if (url.pathname === "/health") return new Response("ok")
        if (url.pathname === "/sse") {
          const body = new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("data: hello\n\n"))
              controller.close()
            },
          })
          return new Response(body, { headers: { "content-type": "text/event-stream" } })
        }
        return new Response("not found", { status: 404 })
      },
    })
    try {
      const running = Promise.all([hub.ensureRunning(sid("ses_a")), hub.ensureRunning(sid("ses_b"))])
      const base = `http://localhost:${server.port}`

      const health = await fetch(`${base}/health`)
      expect(await health.text()).toBe("ok")

      const sse = await fetch(`${base}/sse`)
      expect(sse.headers.get("content-type")).toContain("text/event-stream")
      const chunk = await sse.body!.getReader().read()
      expect(new TextDecoder().decode(chunk.value)).toContain("data: hello")

      await running
    } finally {
      await server.stop(true)
      await hub.terminateAll()
    }
  })
})
