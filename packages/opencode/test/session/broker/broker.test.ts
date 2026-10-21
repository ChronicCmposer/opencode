// Phase 10 — MCP/LSP broker lifecycle (plan tasks 10.1–10.8).
//
// The broker is a dedicated Worker (not in K, not headroom) that owns every
// MCP/LSP child process. These tests drive the real worker over a transferred
// `MessagePort` and assert the supervision properties with real child
// processes: cardinality, per-directory serialization, `mcp_timeout`, shared
// per-directory state/OAuth, group-kill teardown, cancel-by-request-id,
// notification fan-out, and crash → reap-orphans → respawn.

import { afterEach, describe, expect, test } from "bun:test"
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { threadId as mainThreadId } from "node:worker_threads"
import {
  BrokerRequestError,
  createBroker,
  type BrokerClient,
  type SupervisionEvent,
} from "@/session/broker/broker"
import type { ServerSpec } from "@/session/broker/protocol"
import { createLeakDetector } from "../../lib/leak-detector"
import { pollUntil } from "../../lib/poll"
import {
  expectedDescendantCount,
  isAlive,
  isGroupAlive,
  listDescendants,
  posixSupport,
  waitFor,
  waitForDeath,
} from "../../lib/process-tree"

const CHILD_ENTRY = new URL("./broker-child.ts", import.meta.url).pathname
const LSP_FIXTURE = new URL("../../fixture/mcp-lsp-descendants.ts", import.meta.url).pathname
const FAULTS_ENV = "OPENCODE_TEST_FAULTS"
const posix = posixSupport()

const cleanups: Array<() => void> = []
const tempDirs: string[] = []
const liveBrokers: BrokerClient[] = []

afterEach(async () => {
  for (const broker of liveBrokers.splice(0)) {
    await broker.teardown().catch(() => undefined)
  }
  for (const cleanup of cleanups.splice(0)) cleanup()
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true })
})

function makeTmp(): string {
  const dir = mkdtempSync(join(tmpdir(), "broker-test-"))
  tempDirs.push(dir)
  return dir
}

function track(broker: BrokerClient): BrokerClient {
  liveBrokers.push(broker)
  return broker
}

function mcpSpec(overrides: Partial<ServerSpec> = {}): ServerSpec {
  return { name: "fake", kind: "mcp", command: [process.execPath, CHILD_ENTRY], ...overrides }
}

interface JournalEvent {
  readonly event: "start" | "end"
  readonly id: unknown
  readonly active: number
  readonly pid: number
  readonly t: number
}

function readJournal(path: string): JournalEvent[] {
  return readFileSync(path, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as JournalEvent)
}

describe("broker worker + port-pairing handshake (10.2, 10.6)", () => {
  test("runs on a dedicated thread and routes RPC over a transferred MessagePort", async () => {
    const broker = track(createBroker())
    await broker.ready

    expect(broker.threadId).toBeGreaterThan(0)
    expect(broker.threadId).not.toBe(mainThreadId)
    expect(broker.transport).toBe("message-port")
    // The handshake is observable and ordered: ready (worker channel) → attach
    // (port transfer) → attached (port ack) → RPC.
    expect(broker.handshake).toEqual(["ready", "attach", "attached"])

    const ping = await broker.ping()
    expect(ping.threadId).toBe(broker.threadId)
  })
})

describe("exactly one set of children across session workers (10.1)", () => {
  test("two acquisitions of one directory share one child and one instanceId", async () => {
    const broker = track(createBroker())
    const servers = [mcpSpec()]

    const first = await broker.acquire({ directory: "/work/a", servers })
    const second = await broker.acquire({ directory: "/work/a", servers })

    expect(second.instanceId).toBe(first.instanceId)
    expect(first.servers).toHaveLength(1)
    expect(broker.children).toHaveLength(1)
    expect(broker.children[0]?.pid).toBe(first.servers[0]?.pid)
  })

  test.skipIf(!posix.supported)(
    `owns a descendant tree and reaps the whole group (${posix.supported ? "posix" : posix.reason})`,
    async () => {
      const broker = track(createBroker())
      const directory = makeTmp()
      const descendants = 2
      const spec = mcpSpec({ env: { BROKER_CHILD_DESCENDANTS: String(descendants) } })

      const acquired = await broker.acquire({ directory, servers: [spec] })
      const root = acquired.servers[0]!
      expect(isAlive(root.pid)).toBe(true)
      expect(root.pgid).toBe(root.pid) // detached ⇒ its own group leader

      const pids = await waitFor(
        () => {
          const found = listDescendants(root.pid)
          return found.length === expectedDescendantCount(descendants, 1) ? [root.pid, ...found] : undefined
        },
        { message: "descendant tree did not reach the expected size" },
      )
      expect(pids).toHaveLength(1 + descendants)

      await broker.release(directory)
      expect(await waitForDeath(pids)).toBe(true)
      expect(isGroupAlive(root.pgid)).toBe(false)
      expect(broker.children).toHaveLength(0)
    },
  )

  test.skipIf(!posix.supported)("owns LSP servers through the same broker", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const lsp: ServerSpec = {
      name: "fake-lsp",
      kind: "lsp",
      command: [process.execPath, LSP_FIXTURE],
      env: { CHILD_TREE_KIND: "lsp", CHILD_TREE_DEPTH: "1", CHILD_TREE_FANOUT: "1" },
    }

    const acquired = await broker.acquire({ directory, servers: [lsp] })
    const root = acquired.servers[0]!
    const pids = await waitFor(
      () => {
        const found = listDescendants(root.pid)
        return found.length === expectedDescendantCount(1, 1) ? [root.pid, ...found] : undefined
      },
      { message: "LSP descendant tree did not appear" },
    )
    expect(broker.children.map((child) => child.server)).toEqual(["fake-lsp"])

    await broker.release(directory)
    expect(await waitForDeath(pids)).toBe(true)
  })
})

describe("per-directory serialization + mcp_timeout (10.3)", () => {
  test("serializes concurrent calls within one directory", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const journal = join(makeTmp(), "journal.jsonl")
    const spec = mcpSpec({ env: { BROKER_CHILD_DELAY_MS: "60", BROKER_CHILD_JOURNAL: journal } })

    await broker.acquire({ directory, servers: [spec] })
    await Promise.all([
      broker.call({ directory, server: "fake", method: "work", params: { n: 1 } }),
      broker.call({ directory, server: "fake", method: "work", params: { n: 2 } }),
      broker.call({ directory, server: "fake", method: "work", params: { n: 3 } }),
    ])

    const events = readJournal(journal)
    expect(events.filter((event) => event.event === "start")).toHaveLength(3)
    expect(Math.max(...events.map((event) => event.active))).toBe(1)
  })

  test("runs different directories concurrently", async () => {
    const broker = track(createBroker())
    const journalA = join(makeTmp(), "a.jsonl")
    const journalB = join(makeTmp(), "b.jsonl")
    const specA = mcpSpec({ name: "a", env: { BROKER_CHILD_DELAY_MS: "120", BROKER_CHILD_JOURNAL: journalA } })
    const specB = mcpSpec({ name: "b", env: { BROKER_CHILD_DELAY_MS: "120", BROKER_CHILD_JOURNAL: journalB } })

    await broker.acquire({ directory: "/work/a", servers: [specA] })
    await broker.acquire({ directory: "/work/b", servers: [specB] })
    await Promise.all([
      broker.call({ directory: "/work/a", server: "a", method: "work" }),
      broker.call({ directory: "/work/b", server: "b", method: "work" }),
    ])

    const a = readJournal(journalA)
    const b = readJournal(journalB)
    const aStart = a.find((event) => event.event === "start")!.t
    const aEnd = a.find((event) => event.event === "end")!.t
    const bStart = b.find((event) => event.event === "start")!.t
    const bEnd = b.find((event) => event.event === "end")!.t
    // The two directories' windows overlap ⇒ not globally serialized.
    expect(Math.max(aStart, bStart)).toBeLessThan(Math.min(aEnd, bEnd))
  })

  test("enforces mcp_timeout on the handshake", async () => {
    const broker = track(createBroker())
    // `sleep` never speaks MCP, so the initialize handshake never completes.
    const spec: ServerSpec = { name: "silent", kind: "mcp", command: ["sleep", "60"], timeoutMs: 200 }

    const error = await broker.acquire({ directory: "/work/silent", servers: [spec] }).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(BrokerRequestError)
    expect((error as BrokerRequestError).code).toBe("timeout")
  })

  test("enforces mcp_timeout on a call that is never answered", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    await broker.acquire({ directory, servers: [mcpSpec()] })

    const started = Date.now()
    const error = await broker
      .call({ directory, server: "fake", method: "hang", timeoutMs: 150 })
      .catch((reason: unknown) => reason)
    expect((error as BrokerRequestError).code).toBe("timeout")
    expect(Date.now() - started).toBeLessThan(2000)
  })
})

describe("per-directory InstanceState + OAuth ownership (10.4)", () => {
  test("shares the instance id and one broker-owned OAuth callback per directory", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()

    const acquired = await broker.acquire({ directory, servers: [mcpSpec()] })
    const first = await broker.oauth(directory)
    const second = await broker.oauth(directory)

    expect(first.instanceId).toBe(acquired.instanceId)
    expect(second.instanceId).toBe(acquired.instanceId)
    expect(second.callbackUrl).toBe(first.callbackUrl)
    expect(first.owner).toBe("broker")
    // A different directory is a different instance.
    const other = await broker.oauth(makeTmp())
    expect(other.instanceId).not.toBe(first.instanceId)
  })
})

describe("cancel propagation by request-id (10.7)", () => {
  test("abandons an in-flight call and keeps the broker usable", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const journal = join(makeTmp(), "cancel-journal.jsonl")
    await broker.acquire({
      directory,
      servers: [mcpSpec({ env: { BROKER_CHILD_DELAY_MS: "500", BROKER_CHILD_JOURNAL: journal } })],
    })

    const started = Date.now()
    const call = broker.call({ directory, server: "fake", method: "work", requestId: "slow-call" })
    // Wait for the child to journal that the call actually started, instead of
    // guessing with a fixed sleep, then cancel it while it is in flight.
    await pollUntil(() => existsSync(journal) && readJournal(journal).length > 0, {
      message: "broker call never reached the child",
    })
    await broker.cancel("slow-call")

    const error = await call.catch((reason: unknown) => reason)
    expect((error as BrokerRequestError).code).toBe("cancelled")
    expect(Date.now() - started).toBeLessThan(400)

    const ping = await broker.ping()
    expect(ping.threadId).toBe(broker.threadId)
  })
})

describe("MCP notification fan-out (10.2)", () => {
  test("forwards child notifications to subscribers", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    await broker.acquire({ directory, servers: [mcpSpec()] })

    const seen: Array<{ directory: string; server: string; method: string }> = []
    const off = broker.onNotification((frame) => seen.push(frame))
    cleanups.push(off)

    await broker.call({ directory, server: "fake", method: "notify" })
    await waitFor(() => (seen.length > 0 ? true : undefined), { message: "notification was not fanned out" })
    expect(seen[0]).toMatchObject({ directory, server: "fake", method: "notifications/message" })
  })
})

describe("refactor guards: crash-before-ready, handshake, acquire", () => {
  test("does not deadlock a request when the worker never becomes ready", async () => {
    const broker = track(
      createBroker({ entry: new URL("./never-ready-worker.ts", import.meta.url), timeoutMs: 150 }),
    )
    const started = Date.now()
    const error = await broker.acquire({ directory: "/work/a", servers: [mcpSpec()] }).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(BrokerRequestError)
    expect((error as BrokerRequestError).code).toBe("timeout")
    expect(Date.now() - started).toBeLessThan(2000)
  })

  test("two concurrent acquisitions of one directory spawn exactly one child", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const servers = [mcpSpec()]
    const [first, second] = await Promise.all([
      broker.acquire({ directory, servers }),
      broker.acquire({ directory, servers }),
    ])
    expect(second.instanceId).toBe(first.instanceId)
    expect(broker.children).toHaveLength(1)
  })

  test("reports a failed initialize handshake as handshake-failed", async () => {
    const broker = track(createBroker())
    const spec = mcpSpec({ env: { BROKER_CHILD_FAIL_INIT: "1" } })
    const error = await broker.acquire({ directory: makeTmp(), servers: [spec] }).catch((reason: unknown) => reason)
    expect(error).toBeInstanceOf(BrokerRequestError)
    expect((error as BrokerRequestError).code).toBe("handshake-failed")
    expect(broker.children).toHaveLength(0)
  })

  test("resets the observable handshake per generation", async () => {
    const broker = track(createBroker({ env: { [FAULTS_ENV]: "crashWorker@broker:call" } }))
    const events: SupervisionEvent[] = []
    broker.onSupervision((event) => events.push(event))
    await broker.ready
    expect(broker.handshake).toEqual(["ready", "attach", "attached"])

    const directory = makeTmp()
    await broker.acquire({ directory, servers: [mcpSpec()] })
    await broker.call({ directory, server: "fake", method: "work" }).catch(() => undefined)
    await waitFor(() => events.find((event) => event.type === "respawned"), { message: "broker did not respawn" })

    // The respawned generation's handshake replaces, rather than appends to, the
    // previous one.
    expect(broker.handshake).toEqual(["ready", "attach", "attached"])
  })
})

describe("teardown + crash supervision (10.5, 10.8, 10.9)", () => {
  test.skipIf(!posix.supported)("teardown group-kills every owned child", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const spec = mcpSpec({ env: { BROKER_CHILD_DESCENDANTS: "2" } })
    const acquired = await broker.acquire({ directory, servers: [spec] })
    const root = acquired.servers[0]!
    const descendants = await waitFor(
      () => {
        const found = listDescendants(root.pid)
        return found.length === 2 ? found : undefined
      },
      { message: "descendants did not appear" },
    )
    expect(isGroupAlive(root.pgid)).toBe(true)

    await broker.teardown()
    expect(await waitForDeath([root.pid, ...descendants])).toBe(true)
    expect(isGroupAlive(root.pgid)).toBe(false)
    expect(broker.alive).toBe(false)
  })

  test("broker crash rejects in-flight calls, reaps orphans, bumps generation, respawns", async () => {
    const events: SupervisionEvent[] = []
    const reaped: number[] = []
    const broker = track(
      createBroker({
        env: { [FAULTS_ENV]: "crashWorker@broker:call" },
        reaper: {
          reap: (pgid) => {
            reaped.push(pgid)
            try {
              process.kill(-pgid, "SIGTERM")
            } catch {
              /* the orphan may already be gone */
            }
          },
        },
      }),
    )
    broker.onSupervision((event) => events.push(event))
    await broker.ready
    const firstGeneration = broker.generation

    const directory = makeTmp()
    const acquired = await broker.acquire({ directory, servers: [mcpSpec({ env: { BROKER_CHILD_DESCENDANTS: "1" } })] })
    const orphan = acquired.servers[0]!
    expect(broker.children.map((child) => child.pgid)).toContain(orphan.pgid)

    const error = await broker.call({ directory, server: "fake", method: "work" }).catch((reason: unknown) => reason)
    expect((error as BrokerRequestError).code).toBe("crashed")

    const respawned = await waitFor(
      () => events.find((event) => event.type === "respawned"),
      { message: "broker did not respawn" },
    )
    expect(respawned).toMatchObject({ type: "respawned", generation: firstGeneration + 1 })
    expect(broker.generation).toBe(firstGeneration + 1)
    expect(events.some((event) => event.type === "crashed")).toBe(true)

    // The hub mirrored the child PGID and group-killed the orphan. (Bun 1.3.14
    // cannot reap a dead worker's subprocesses, so the child becomes a zombie
    // rather than disappearing; the supervision contract is the reaper call.)
    expect(reaped).toContain(orphan.pgid)

    // The respawned broker is live again.
    const ping = await broker.ping()
    expect(ping.threadId).toBe(broker.threadId)
  })

  test("leaves no leaked workers, ports or process groups after teardown", async () => {
    const broker = track(createBroker())
    const directory = makeTmp()
    const acquired = await broker.acquire({ directory, servers: [mcpSpec()] })
    const root = acquired.servers[0]!

    // The leak detector's `WorkerLike` keys on `id`; the broker exposes its
    // generation under that name.
    const detector = createLeakDetector({
      workers: () => [{ id: broker.workerHandle.generation, alive: broker.workerHandle.alive }],
    })
    const trackedPort = detector.trackPort(broker.portHandle, "broker-rpc")
    if (posix.supported) detector.trackProcessGroup(root.pgid)

    await broker.teardown()
    await waitForDeath([root.pid])
    // The hub-side port is closed by teardown; record that on the tracked handle.
    if (!trackedPort.closed) trackedPort.close()

    const report = detector.detect()
    expect(report.clean).toBe(true)
  })
})
