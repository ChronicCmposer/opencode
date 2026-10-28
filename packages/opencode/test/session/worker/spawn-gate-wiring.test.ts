// Spawn-gate wiring: a session worker's Bun isolate is constructed only after
// the one global spawn permit is granted, and the permit is released only when
// the isolate announces boot-ready (its validated `ready` frame, which arrives
// after the port transfers). A spawn timeout or synchronous constructor throw
// fails that one spawn/session dispatch, never the shared permit or later
// sessions, and a crash respawn goes through the same gate.

import { describe, expect, test } from "bun:test"
import { BOOT_CANCELLED } from "@/session/worker/cancel"
import { createSpawnGate, SpawnGateTimeoutError } from "@/session/worker/spawn-gate"
import { createBunExecutorTransport, spawnBunExecutorWorker } from "@/session/worker/hub"
import type { SessionExecutorInbound } from "@/session/worker/execute"
import { resolveBuildHash } from "@/session/worker/build-hash"
import { flush, pollUntil, settleOf, sid, withDeadline } from "../../lib/async"

/** A valid boot-ready frame for a fake isolate (must carry the hub build hash). */
const ready = (threadId: number) => ({ type: "ready", threadId, buildHash: resolveBuildHash({ env: {} }).hash })

class FakeWorker {
  static instances: FakeWorker[] = []
  /** When set, the next constructor throws synchronously (invalid entry, EAGAIN…). */
  static constructError: string | undefined
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: ((event: ErrorEvent) => void) | null = null
  readonly posted: unknown[] = []
  terminated = false

  constructor(
    readonly url: string | URL,
    readonly options?: unknown,
  ) {
    if (FakeWorker.constructError !== undefined) throw new Error(FakeWorker.constructError)
    FakeWorker.instances.push(this)
  }

  postMessage(message: unknown): void {
    this.posted.push(message)
  }

  emit(data: unknown): void {
    this.onmessage?.({ data } as MessageEvent)
  }

  fail(message: string): void {
    this.onerror?.({ message, error: new Error(message) } as ErrorEvent)
  }

  async terminate(): Promise<number> {
    this.terminated = true
    return 0
  }

  static reset(): void {
    FakeWorker.instances = []
    FakeWorker.constructError = undefined
  }
}

async function withFakeWorker(run: () => Promise<void>): Promise<void> {
  const original = (globalThis as { Worker?: unknown }).Worker
  FakeWorker.reset()
  ;(globalThis as { Worker?: unknown }).Worker = FakeWorker
  try {
    await run()
  } finally {
    ;(globalThis as { Worker?: unknown }).Worker = original
  }
}

const runFrame = (id: number, session: string): SessionExecutorInbound => ({
  type: "run",
  id,
  sessionID: sid(session),
  kind: "session",
})

describe("spawn-gate wiring", () => {
  test("concurrent cold sessions serialize construction and release only on boot-ready", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const first = createBunExecutorTransport({ entry: "fake://first", spawnGate: gate })
      const second = createBunExecutorTransport({ entry: "fake://second", spawnGate: gate })
      const firstReady = settleOf(first.waitFor((message) => message.type === "ready"))
      const secondReady = settleOf(second.waitFor((message) => message.type === "ready"))

      first.post(runFrame(1, "ses_first"))
      second.post(runFrame(2, "ses_second"))
      await flush()

      expect(FakeWorker.instances).toHaveLength(1)
      const firstWorker = FakeWorker.instances[0]!
      expect(firstWorker.url).toBe("fake://first")
      expect(firstWorker.posted).toEqual([runFrame(1, "ses_first")])

      // The first isolate boots; only now may the second construct.
      firstWorker.emit(ready(1))
      await flush()

      expect(FakeWorker.instances).toHaveLength(2)
      const secondWorker = FakeWorker.instances[1]!
      expect(secondWorker.url).toBe("fake://second")
      expect(secondWorker.posted).toEqual([runFrame(2, "ses_second")])

      secondWorker.emit(ready(2))
      await flush()
      expect((await firstReady).ok).toBe(true)
      expect((await secondReady).ok).toBe(true)
      await first.terminate()
      await second.terminate()
    })
  })

  test("a boot cancel while waiting for the global permit resolves at once and never constructs", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const holder = createBunExecutorTransport({ entry: "fake://holder", spawnGate: gate })
      holder.post(runFrame(1, "ses_holder"))
      await flush()
      expect(FakeWorker.instances).toHaveLength(1)

      const waiting = createBunExecutorTransport({ entry: "fake://waiting", spawnGate: gate })
      waiting.post(runFrame(2, "ses_waiting"))
      await flush()
      // The waiter has not been granted the permit, so no isolate exists.
      expect(FakeWorker.instances).toHaveLength(1)

      // The cancel must resolve NOW, not when the holder eventually releases the
      // permit (which may be its whole boot).
      const cancelled = await withDeadline(
        waiting.cancelBoot?.() ?? Promise.resolve(undefined),
        200,
        "cancelBoot waited for the global spawn permit",
      )
      expect(cancelled).toEqual(BOOT_CANCELLED)

      // Releasing the holder's permit must not construct the cancelled waiter.
      FakeWorker.instances[0]!.emit(ready(1))
      await flush()
      expect(FakeWorker.instances.map((instance) => instance.url)).toEqual(["fake://holder"])
      expect(waiting.alive).toBe(false)

      await holder.terminate()
    })
  })

  test("a malformed ports frame fails the spawn loud and releases the gate", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const transport = createBunExecutorTransport({ entry: "fake://bad-ports", spawnGate: gate })
      const run = settleOf(transport.waitFor(() => true))
      transport.post(runFrame(1, "ses_bad"))
      await flush()

      const worker = FakeWorker.instances[0]!
      worker.emit({ type: "ports", egress: "not-a-port" })

      const settled = await run
      expect(settled).toMatchObject({ ok: false })
      expect(settled.ok ? undefined : settled.error).toBeInstanceOf(Error)
      expect(transport.alive).toBe(false)
      await flush()
      // Never ready: the isolate cannot have opened its DB/WAL, so the teardown
      // is terminate-only (no orderly shutdown frame).
      expect(worker.posted.some((frame) => (frame as { type?: string }).type === "shutdown")).toBe(false)

      // The failed spawn released the permit for the next session.
      const next = createBunExecutorTransport({ entry: "fake://after", spawnGate: gate })
      next.post(runFrame(2, "ses_after"))
      await flush()
      expect(FakeWorker.instances.map((instance) => instance.url)).toEqual(["fake://bad-ports", "fake://after"])
      await next.terminate()
    })
  })

  test("a port-less hub-proxy or malformed ready frame fails the spawn loud", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const badProxy = createBunExecutorTransport({ entry: "fake://bad-proxy", spawnGate: gate })
      const proxyRun = settleOf(badProxy.waitFor(() => true))
      badProxy.post(runFrame(1, "ses_proxy"))
      await flush()
      FakeWorker.instances[0]!.emit({ type: "hub-proxy" })

      const proxySettled = await proxyRun
      expect(proxySettled).toMatchObject({ ok: false })
      expect(badProxy.alive).toBe(false)

      const badReady = createBunExecutorTransport({ entry: "fake://bad-ready", spawnGate: gate })
      const readyRun = settleOf(badReady.waitFor(() => true))
      badReady.post(runFrame(2, "ses_ready"))
      await flush()
      // A `ready` without the numeric thread id is not a valid handshake, but
      // the isolate DID boot (it opened its DB/WAL), so its teardown is orderly.
      const malformedReadyWorker = FakeWorker.instances[1]!
      malformedReadyWorker.emit({ type: "ready" })

      const readySettled = await readyRun
      expect(readySettled).toMatchObject({ ok: false })
      expect(badReady.alive).toBe(false)
      await pollUntil(
        () => malformedReadyWorker.posted.some((frame) => (frame as { type?: string }).type === "shutdown"),
        { message: "a rejected ready frame must close through the orderly shutdown frame" },
      )
      expect(malformedReadyWorker.terminated).toBe(true)
      await badProxy.terminate()
      await badReady.terminate()
    })
  })

  test("a ready frame with the wrong build hash fails the spawn loud", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const transport = createBunExecutorTransport({ entry: "fake://wrong-hash", spawnGate: gate })
      const run = settleOf(transport.waitFor(() => true))
      transport.post(runFrame(1, "ses_hash"))
      await flush()
      const worker = FakeWorker.instances[0]!
      worker.emit({ type: "ready", threadId: 1, buildHash: "not-the-hub-hash" })

      const settled = await run
      expect(settled.ok).toBe(false)
      if (!settled.ok) expect(String(settled.error)).toContain("build mismatch")
      expect(transport.alive).toBe(false)
      // The bundle booted (its native WAL handle exists) but the hub rejected the
      // build: close it through the shutdown frame before terminate, never
      // terminate-only, or the WAL lock outlives the thread.
      await pollUntil(
        () => worker.posted.some((frame) => (frame as { type?: string }).type === "shutdown"),
        { message: "a build-mismatched ready frame must close through the orderly shutdown frame" },
      )
      expect(worker.terminated).toBe(true)
      await transport.terminate()
    })
  })

  test("a spawn timeout fails the spawn, not later sessions", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 20, capacity: 1 })
      const holder = createBunExecutorTransport({ entry: "fake://holder", spawnGate: gate })
      holder.post(runFrame(1, "ses_holder"))
      await flush()
      const holderWorker = FakeWorker.instances[0]!

      const blocked = createBunExecutorTransport({ entry: "fake://blocked", spawnGate: gate })
      blocked.post(runFrame(2, "ses_blocked"))
      const timedOut = await settleOf(blocked.waitFor(() => true))

      expect(timedOut).toMatchObject({ ok: false })
      expect(timedOut.ok ? undefined : timedOut.error).toBeInstanceOf(SpawnGateTimeoutError)
      expect(blocked.alive).toBe(false)

      // The timeout released nothing it did not hold: boot-ready still frees the
      // permit, and the next session spawns normally.
      holderWorker.emit(ready(1))
      await flush()

      const next = createBunExecutorTransport({ entry: "fake://next", spawnGate: gate })
      next.post(runFrame(3, "ses_next"))
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual([
        "fake://holder",
        "fake://next",
      ])
      FakeWorker.instances[1]!.emit(ready(2))
      await flush()
      await holder.terminate()
      await next.terminate()
    })
  })

  test("a crash respawn acquires the same gate", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const first = spawnBunExecutorWorker(1, { entry: "fake://first", workKind: "session", spawnGate: gate })
      const firstRun = settleOf(first.execute({ sessionID: sid("ses_first"), depth: 0 }))
      await flush()
      const firstWorker = FakeWorker.instances[0]!
      firstWorker.emit(ready(1))

      // Crash the isolate and retire it, as the dispatcher does at-most-once.
      firstWorker.fail("isolate crashed")
      expect((await firstRun).ok).toBe(false)
      await first.terminate()

      // A respawn is a fresh worker; it must wait behind the permit.
      const held = await gate.acquire()
      const respawn = spawnBunExecutorWorker(2, {
        entry: "fake://respawn",
        workKind: "session",
        spawnGate: gate,
      })
      const respawnRun = settleOf(respawn.execute({ sessionID: sid("ses_respawn"), depth: 0 }))
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual(["fake://first"])

      held.release()
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual(["fake://first", "fake://respawn"])
      const respawnWorker = FakeWorker.instances[1]!
      respawnWorker.emit(ready(2))
      const run = respawnWorker.posted[0] as { readonly id: number }
      respawnWorker.emit({ type: "done", id: run.id, result: { sessionID: "ses_respawn" } })
      expect((await respawnRun).ok).toBe(true)
      await respawn.terminate()
    })
  })

  test("a synchronous constructor throw releases the permit for the next spawn", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 50 })
      FakeWorker.constructError = "invalid worker entry"
      const doomed = createBunExecutorTransport({ entry: "fake://doomed", spawnGate: gate })
      doomed.post(runFrame(1, "ses_doomed"))
      const failed = await settleOf(doomed.waitFor(() => true))

      expect(failed).toMatchObject({ ok: false })
      expect(failed.ok ? undefined : failed.error).toMatchObject({ message: "invalid worker entry" })
      expect(doomed.alive).toBe(false)

      // The permit was released despite the constructor throwing: the next spawn
      // acquires immediately instead of waiting out the gate timeout.
      FakeWorker.constructError = undefined
      const next = createBunExecutorTransport({ entry: "fake://next", spawnGate: gate })
      next.post(runFrame(2, "ses_next"))
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual(["fake://next"])

      FakeWorker.instances[0]!.emit(ready(1))
      await flush()
      await next.terminate()
    })
  })

  test("a ports frame keeps the gate held and the boot timer armed until ready", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const first = createBunExecutorTransport({ entry: "fake://first", spawnGate: gate })
      first.post(runFrame(1, "ses_first"))
      await flush()
      const firstWorker = FakeWorker.instances[0]!

      // Production transfers ports before the executor announces ready.
      firstWorker.emit({ type: "ports" })
      await flush()
      expect(FakeWorker.instances).toHaveLength(1)

      const second = createBunExecutorTransport({ entry: "fake://second", spawnGate: gate })
      second.post(runFrame(2, "ses_second"))
      await flush()
      expect(FakeWorker.instances).toHaveLength(1)

      firstWorker.emit(ready(1))
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual(["fake://first", "fake://second"])

      FakeWorker.instances[1]!.emit(ready(2))
      await flush()
      await first.terminate()
      await second.terminate()
    })
  })

  test("a ports frame followed by no ready times out boot and releases the gate", async () => {
    await withFakeWorker(async () => {
      const gate = createSpawnGate({ timeoutMs: 1_000, capacity: 1 })
      const hung = createBunExecutorTransport({
        entry: "fake://hung",
        spawnGate: gate,
        bootTimeoutMs: 20,
      })
      hung.post(runFrame(1, "ses_hung"))
      await flush()
      const hungWorker = FakeWorker.instances[0]!
      hungWorker.emit({ type: "ports" })

      // A generous reply deadline so the boot timer (20ms) is the failure under test.
      const result = await settleOf(hung.waitFor(() => true, { timeoutMs: 200 }))
      expect(result).toMatchObject({ ok: false })
      expect(result.ok ? undefined : result.error).toMatchObject({
        message: "session worker boot timeout after 20ms",
      })
      expect(hung.alive).toBe(false)
      expect(hungWorker.terminated).toBe(true)

      // The timed-out spawn released the global permit for later sessions.
      const next = createBunExecutorTransport({ entry: "fake://next", spawnGate: gate })
      next.post(runFrame(2, "ses_next"))
      await flush()
      expect(FakeWorker.instances.map((worker) => worker.url)).toEqual(["fake://hung", "fake://next"])

      FakeWorker.instances[1]!.emit(ready(2))
      await flush()
      await next.terminate()
    })
  })
})
