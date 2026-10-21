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
import { createBunExecutorTransport, createBunExecutorWorkerPool, createSessionHub } from "@/session/worker/hub"
import type { ExecutorTransport, SessionExecutorInbound, SessionExecutorOutbound } from "@/session/worker/execute"
import { testEffect } from "../../lib/effect"
import { TestLLMServer } from "../../lib/llm-server"
import { sid } from "../../lib/async"
import { pollUntil } from "../../lib/poll"
import { workerSnapshot } from "../../lib/snapshot"

const it = testEffect(TestLLMServer.layer)

/** The dependency-light executor fixture (Phase 5). */
const EXECUTOR_FIXTURE = new URL("../../fixture/session-execute-worker.ts", import.meta.url)
/** The production worker entry (reduced layer as the work factory). */
const SESSION_WORKER = new URL("../../../src/session/worker/session-worker.ts", import.meta.url)

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
          snapshot: workerSnapshot(),
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
      await pollUntil(() => hub.dispatcher.registry.get(sid("ses_parent"))?.state === "running", {
        message: "parent never ran",
      })

      const sibling = hub.dispatcher.dispatch({ sessionID: sid("ses_sib"), depth: 0 })
      await pollUntil(() => hub.dispatcher.queuedCount === 1, { message: "sibling never queued" })
      expect(hub.dispatcher.queuedCount).toBe(1)
      expect(hub.dispatcher.headroomSlotCount).toBe(0)

      const child = hub.dispatcher.dispatch({ sessionID: sid("ses_child"), depth: 1 })
      await pollUntil(() => hub.dispatcher.headroomSlotCount === 1, { message: "headroom never spawned" })

      // The awaiting parent keeps its foreground slot while the child runs in headroom.
      expect(hub.dispatcher.registry.get(sid("ses_parent"))?.state).toBe("running")
      expect(hub.dispatcher.registry.get(sid("ses_child"))?.state).toBe("running")

      await Promise.all([parent, sibling, child])
      expect(hub.dispatcher.busySlotCount).toBe(0)
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
      expect(hub.dispatcher.busySlotCount).toBe(0)
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

// ---------------------------------------------------------------------------
// Transport frame buffering: consume-on-match, cap, crash teardown.
// ---------------------------------------------------------------------------

const markerOf = (message: SessionExecutorOutbound): number | undefined =>
  (message as unknown as { readonly marker?: number }).marker

class FakeWorker {
  static last: FakeWorker | undefined
  onmessage: ((event: { data: unknown }) => void) | null = null
  onerror: ((event: { message: string }) => void) | null = null
  readonly posted: unknown[] = []
  terminateCount = 0

  constructor() {
    FakeWorker.last = this
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  terminate(): Promise<number> {
    this.terminateCount += 1
    return Promise.resolve(0)
  }

  emit(data: unknown): void {
    this.onmessage?.({ data })
  }

  fail(message: string): void {
    this.onerror?.({ message })
  }
}

async function withFakeWorker(
  run: (fake: FakeWorker, transport: ExecutorTransport) => Promise<void>,
  options: { readonly bufferCapacity?: number } = {},
): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    const transport = createBunExecutorTransport({
      entry: "fake://session-worker",
      ...(options.bufferCapacity === undefined ? {} : { bufferCapacity: options.bufferCapacity }),
    })
    const fake = FakeWorker.last
    if (fake === undefined) throw new Error("fake worker was never constructed")
    await run(fake, transport)
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

describe("12.x executor transport buffering", () => {
  test("a buffered frame is consumed by the first matching wait", async () => {
    await withFakeWorker(async (fake, transport) => {
      fake.emit({ marker: 1 })
      const first = await transport.waitFor((message) => markerOf(message) === 1)
      expect(markerOf(first)).toBe(1)
      await expect(transport.waitFor((message) => markerOf(message) === 1, { timeoutMs: 20 })).rejects.toThrow(
        /timed out/,
      )
    })
  })

  test("the buffer is capped, dropping the oldest frames", async () => {
    await withFakeWorker(
      async (fake, transport) => {
        fake.emit({ marker: 1 })
        fake.emit({ marker: 2 })
        fake.emit({ marker: 3 })
        await expect(transport.waitFor((message) => markerOf(message) === 1, { timeoutMs: 20 })).rejects.toThrow(
          /timed out/,
        )
        const latest = await transport.waitFor((message) => markerOf(message) === 3)
        expect(markerOf(latest)).toBe(3)
      },
      { bufferCapacity: 2 },
    )
  })

  test("a worker error terminates the isolate, detaches handlers and fails waiters", async () => {
    await withFakeWorker(async (fake, transport) => {
      const pending = transport.waitFor(() => true)
      fake.fail("boom")

      await expect(pending).rejects.toThrow("session worker error: boom")
      expect(fake.terminateCount).toBe(1)
      expect(fake.onmessage).toBeNull()
      expect(fake.onerror).toBeNull()
      expect(transport.alive).toBe(false)
      expect(() =>
        transport.post({ type: "run", id: 1, sessionID: "ses_1" } as unknown as SessionExecutorInbound),
      ).toThrow(/terminated/)
    })
  })

  test("a malformed workerData fails loud at the isolate boundary", async () => {
    const transport = createBunExecutorTransport({
      entry: SESSION_WORKER,
      timeoutMs: 5_000,
      workerData: { snapshot: null, env: {}, model: { baseUrl: "http://localhost:1" } },
    })
    await expect(transport.waitFor(() => true)).rejects.toThrow(/snapshot is required/)
    await transport.terminate()
  })
})
