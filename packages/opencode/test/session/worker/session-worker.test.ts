// Wave 2 — session-worker runtime tests (TDD plan steps 22, 27, 28).
//
// The production entry is import-safe outside a worker (it only bootstraps when
// `workerData` is present), so the pure aggregation / registry / work builders
// are exercised in-process here.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Latch, Layer, Option, Schema, Sink, Stream } from "effect"
import { ChildProcessSpawner } from "effect/unstable/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { ShellTool } from "@/tool/shell"
import { EventV2 } from "@opencode-ai/core/event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
import { serveWriterPort, type HubWriter } from "@/session/writer/client"
import type { EventV2Event } from "@/session/worker/frame"
import { collectNodeNames } from "@/session/worker/node-table"
import {
  EventEgressClientNode,
  EventEgressClientService,
  ModelClient,
  PIPELINE_SESSION_NODES,
  RUNNABLE_SESSION_NODES,
  WorkerModelMismatchError,
  buildSessionWorkerLayer,
  createHubProxyClient,
  serveHubProxy,
  type SessionLayerOptions,
  type WorkerModelConfig,
} from "@/session/worker/layer"
import {
  DEFAULT_WORK_KIND,
  UnknownWorkKindError,
  type SessionRunControl,
  type SessionWork,
} from "@/session/worker/execute"
import {
  MissingWorkerPortError,
  bootstrapSessionWorker,
  buildWorkerClients,
  createControlPidReporter,
  createSessionWork,
  createWorkRegistry,
  parseSessionWorkerInit,
  productionWorkRegistry,
  reportedWorkerID,
  workerGraphReplacements,
  workerInstanceContext,
  writerReadMessages,
  type SessionWorkerInit,
  type SessionWorkerModel,
} from "@/session/worker/session-worker"
import type { JobInfo } from "@/session/worker/job-registry"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import {
  createPidRegistry,
  registerReported,
  PidReporter,
  type PidReport,
  type PidReporterInterface,
} from "@/session/worker/pid-registry"
import { reportDetachedShell } from "@/tool/shell"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Truncate } from "@/tool/truncate"
import { Plugin } from "@/plugin"
import { Config } from "@/config/config"
import { Agent } from "@/agent/agent"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { provideInstance, testInstanceStoreLayer } from "../../fixture/fixture"
import type { WorkerPlugin } from "@/session/worker/plugin-host"
import { Snapshot } from "@/snapshot"
import { GlobalBus } from "@/bus/global"
import { createHubEventSink } from "@/session/worker/hub"
import { workerSnapshot } from "../../lib/snapshot"
import { withParts } from "../../lib/session-parts"
import { startMockProvider } from "../../lib/mock-provider"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { assertStructuredCloneable } from "../../lib/session-worker-init"

const sid = (value: string) => SessionID.make(value)

const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
  broker: { call: async () => null },
  backgroundJob: { wait: async () => null, cancel: async () => {} },
})

const modelLayer = (text: string) =>
  Layer.succeed(ModelClient, {
    complete: () => Effect.succeed({ text }),
  })

const buildLayer = (overrides: Partial<SessionLayerOptions> = {}) =>
  buildSessionWorkerLayer({
    // The pipeline tests exercise the reduced one-shot seam explicitly; the
    // production default is the full runnable graph.
    nodes: PIPELINE_SESSION_NODES,
    snapshot: workerSnapshot(),
    env: { values: {} },
    clients: stubClients(),
    modelClient: modelLayer("worker text"),
    ...overrides,
  })

describe("bootstrapSessionWorker control-port transfer (steps 22 / Wave 4)", () => {
  const fakeScope = () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    return { frames, scope }
  }

  const init = (capabilities?: { sessionWorkerPorts?: boolean }) => ({
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: {
      providerID: "mock",
      modelID: "m",
      apiId: "m",
      baseUrl: "http://127.0.0.1:1/v1",
      apiKey: "k",
      capabilities: { temperature: true, reasoning: false },
      limit: { context: 128_000, output: 4096 },
    },
    ...(capabilities === undefined ? {} : { capabilities }),
  })

  const frameTypes = (frames: ReadonlyArray<{ data: unknown }>): string[] =>
    frames.map((frame) => (frame.data as { type?: string }).type ?? "")

  test("transfers job, worker and hub-proxy ports when the capability is advertised", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const types = frameTypes(frames)
    // The dedicated job control link is always transferred; the writer/egress/
    // broker bundle and the hub-proxy control port join it for a full hub.
    expect(types).toContain("job-port")
    expect(types).toContain("ports")
    expect(types).toContain("hub-proxy")
    expect(types).toContain("ready")
    expect(typeof scope.onmessage).toBe("function")
  })

  test("the hub-proxy frame carries a transferred MessagePort", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const hubProxy = frames.find((frame) => (frame.data as { type?: string }).type === "hub-proxy")
    expect(hubProxy).toBeDefined()
    const port = (hubProxy!.data as { port?: unknown }).port
    expect(port).toBeInstanceOf(MessagePort)
    expect(hubProxy!.transfer).toHaveLength(1)
  })

  test("degrades to the reduced pipeline without the capability", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init(), scope)
    const types = frameTypes(frames)
    expect(types).toContain("job-port")
    expect(types).toContain("ready")
    // No served ports: transferring one nobody serves would hang a session.
    expect(types).not.toContain("ports")
    expect(types).not.toContain("hub-proxy")
  })

  test("transfers the dedicated egress control (mesh) port for a full hub", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const ports = frames.find((frame) => (frame.data as { type?: string }).type === "ports")
    expect(ports).toBeDefined()
    // The control plane rides a SEPARATE port so durable wakeups/resync never
    // queue behind deltas; without it the egress mesh is dead in production.
    const control = (ports!.data as { egressControl?: unknown }).egressControl
    expect(control).toBeInstanceOf(MessagePort)
    // writer + egress + egressControl + broker are all transferred.
    expect(ports!.transfer).toHaveLength(4)
  })

  test("wires a real logger into the E2 client lifecycles (not the noop default)", () => {
    const { scope } = fakeScope()
    const lines: Array<{ message: string; fields: Record<string, unknown> }> = []
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope, {
      log: (message, fields) => void lines.push({ message, fields }),
    })
    const messages = lines.map((entry) => entry.message)
    // Each port-backed peer emits its E2 connected line at construction; without
    // a real logger these were dormant in production.
    expect(messages).toContain(workerLogMessage("writer connected"))
    expect(messages).toContain(workerLogMessage("egress connected"))
    expect(messages).toContain(workerLogMessage("broker connected"))
  })
})

// ---------------------------------------------------------------------------
// Child-result routing (9.4). The hub registry pushes a settled child's info to
// the worker hosting its `parentID`; production must have the subscriber
// installed or the push is silently dropped.
// ---------------------------------------------------------------------------

describe("child-result subscription (9.4)", () => {
  const fakeScope = () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    return { frames, scope }
  }

  const init = () => ({
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: {
      providerID: "mock",
      modelID: "m",
      apiId: "m",
      baseUrl: "http://127.0.0.1:1/v1",
      apiKey: "k",
      capabilities: { temperature: true, reasoning: false },
      limit: { context: 128_000, output: 4096 },
    },
    capabilities: { sessionWorkerPorts: true },
  })

  test("production installs the subscriber the hub routes settled children to", async () => {
    const { frames, scope } = fakeScope()
    const bootstrap = bootstrapSessionWorker(init(), scope)
    const received = new Promise<JobInfo>((resolve) => bootstrap.onChildResult(resolve))

    // Drive the hub half of the transferred job port: the registry pushes a
    // settled child keyed by the parent session.
    const jobFrame = frames.find((frame) => (frame.data as { type?: string }).type === "job-port")
    expect(jobFrame).toBeDefined()
    const hubPort = (jobFrame!.data as { port: MessagePort }).port
    hubPort.start()
    const result: JobInfo = {
      id: "ses_child",
      type: "task",
      status: "completed",
      started_at: 1,
      completed_at: 2,
      output: "done",
      parentID: "ses_parent",
      depth: 1,
    }
    hubPort.postMessage({ kind: "push", type: "childResult", parentID: "ses_parent", result })

    expect(await received).toMatchObject({ id: "ses_child", parentID: "ses_parent", status: "completed" })
    bootstrap.dispose()
    hubPort.close()
  })

  test("a disposed bootstrap stops delivering child results", async () => {
    const { frames, scope } = fakeScope()
    const bootstrap = bootstrapSessionWorker(init(), scope)
    let delivered = 0
    bootstrap.onChildResult(() => void (delivered += 1))
    bootstrap.dispose()

    const jobFrame = frames.find((frame) => (frame.data as { type?: string }).type === "job-port")!
    const hubPort = (jobFrame.data as { port: MessagePort }).port
    hubPort.start()
    hubPort.postMessage({
      kind: "push",
      type: "childResult",
      parentID: "ses_parent",
      result: { id: "ses_child", type: "task", status: "completed", started_at: 1, depth: 1 },
    })
    // Ordering barrier: the port's message task is queued before this timer, so
    // by the time it fires the (removed) handler would already have run.
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(delivered).toBe(0)
    hubPort.close()
  })
})

// ---------------------------------------------------------------------------
// Step 22 — client aggregation.
// ---------------------------------------------------------------------------

describe("buildWorkerClients (step 22)", () => {
  const emptyChannels = () => {
    const writer = new MessageChannel()
    const egress = new MessageChannel()
    const broker = new MessageChannel()
    const backgroundJob = new MessageChannel()
    return { writer, egress, broker, backgroundJob }
  }

  test("every port is required and a missing one fails loud", () => {
    expect(() => buildWorkerClients({})).toThrow(MissingWorkerPortError)
    expect(() => buildWorkerClients({})).toThrow(/writer/)

    const channels = emptyChannels()
    expect(() => buildWorkerClients({ writer: channels.writer.port1 })).toThrow(/egress/)
    expect(() => buildWorkerClients({ writer: channels.writer.port1, egress: channels.egress.port1 })).toThrow(/broker/)
    expect(() =>
      buildWorkerClients({
        writer: channels.writer.port1,
        egress: channels.egress.port1,
        broker: channels.broker.port1,
      }),
    ).toThrow(/backgroundJob/)
  })

  test("wires the four clients and round-trips writer publish/query over the port", async () => {
    const channels = emptyChannels()
    const published: string[] = []
    const hubWriter: HubWriter = {
      query: async () => [{ value: 1 }],
      publish: async (event) => {
        published.push(event.type)
        return { seq: 9, committed: true }
      },
      begin: async () => {
        throw new Error("begin was not expected in this test")
      },
      txQuery: async () => {
        throw new Error("txQuery was not expected in this test")
      },
      commit: async () => {
        throw new Error("commit was not expected in this test")
      },
      rollback: async () => {
        throw new Error("rollback was not expected in this test")
      },
    }
    serveWriterPort(hubWriter, channels.writer.port2)

    const clients = buildWorkerClients({
      writer: channels.writer.port1,
      egress: channels.egress.port1,
      broker: channels.broker.port1,
      backgroundJob: channels.backgroundJob.port1,
    })

    expect(typeof clients.writer.publish).toBe("function")
    expect(typeof clients.egress.emit).toBe("function")
    expect(typeof clients.broker.call).toBe("function")
    expect(typeof clients.backgroundJob.wait).toBe("function")

    const result = await clients.writer.publish({
      id: "e1",
      type: "message.assistant",
      aggregateID: "s1",
      data: { text: "hi" },
    })
    expect(result).toEqual({ seq: 9, committed: true })
    expect(published).toEqual(["message.assistant"])

    expect(await clients.writer.query("all", "SELECT 1")).toEqual([{ value: 1 }])
  })

  test("wires the egress control port into the mesh client", () => {
    const channels = emptyChannels()
    const control = new MessageChannel()
    const clients = buildWorkerClients({
      writer: channels.writer.port1,
      egress: channels.egress.port1,
      egressControl: control.port1,
      broker: channels.broker.port1,
      backgroundJob: channels.backgroundJob.port1,
    })
    // The dedicated control port gives the egress client a live mesh (durable
    // ack/resync); without it `mesh` is `undefined` and the control plane dies.
    expect(clients.egress.mesh).toBeDefined()
  })

  test("omits the mesh client when no control port is supplied", () => {
    const channels = emptyChannels()
    const clients = buildWorkerClients({
      writer: channels.writer.port1,
      egress: channels.egress.port1,
      broker: channels.broker.port1,
      backgroundJob: channels.backgroundJob.port1,
    })
    expect(clients.egress.mesh).toBeUndefined()
  })

  test("forwards a supplied log into every peer's E2 lifecycle", () => {
    const channels = emptyChannels()
    const lines: Array<{ message: string; fields: Record<string, unknown> }> = []
    const log: WorkerLogger = (message, fields) => void lines.push({ message, fields })
    const clients = buildWorkerClients(
      {
        writer: channels.writer.port1,
        egress: channels.egress.port1,
        broker: channels.broker.port1,
        backgroundJob: channels.backgroundJob.port1,
      },
      { workerID: 4, log },
    )
    expect(typeof clients.writer.publish).toBe("function")
    const connected = lines.filter((entry) => entry.message.endsWith("connected"))
    expect(connected.map((entry) => entry.message)).toEqual([
      workerLogMessage("writer connected"),
      workerLogMessage("egress connected"),
      workerLogMessage("broker connected"),
    ])
    for (const entry of connected) expect(entry.fields.workerID).toBe(4)
  })
})

// ---------------------------------------------------------------------------
// P5 prerequisite — worker→hub PID reporting.
//
// A worker's shell tool spawns a detached child (its own process group). The
// worker reports `{ pid, pgid, label }` over its control/hub-proxy port so the
// hub's `PidRegistry` mirrors it and a terminate can group-kill the orphan.
// ---------------------------------------------------------------------------

/** A fake spawner whose child has a fixed PID (and therefore PGID when detached). */
function fixedPidSpawnerLayer(pid: number): Layer.Layer<ChildProcessSpawner.ChildProcessSpawner> {
  const spawner = ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(pid),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: Sink.drain as never,
        stdout: Stream.empty,
        stderr: Stream.empty,
        all: Stream.empty,
        getInputFd: () => Sink.drain as never,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      }),
    ),
  )
  return Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner)
}

/**
 * A minimal ShellTool graph. The real spawner is replaced by a fixed-PID fake
 * so the report is deterministic and no process is spawned; the `PidReporter`
 * is merged at the top so the shell tool sees it at execution time.
 */
function shellToolHarness(
  spawnerLayer: Layer.Layer<ChildProcessSpawner.ChildProcessSpawner>,
  reporter: PidReporterInterface,
) {
  return Layer.mergeAll(
    LayerNode.compile(
      LayerNode.group([
        CrossSpawnSpawner.node,
        FSUtil.node,
        Plugin.node,
        Truncate.node,
        Config.node,
        Agent.node,
        RuntimeFlags.node,
      ]),
      [[CrossSpawnSpawner.node, spawnerLayer]],
    ),
    Layer.succeed(PidReporter, reporter),
    testInstanceStoreLayer,
  )
}

describe("worker→hub PID reporting (P5 prerequisite)", () => {
  test("the control reporter posts the frozen pid-report frame over the port", async () => {
    const channel = new MessageChannel()
    const received = new Promise<PidReport>((resolve) => {
      channel.port2.onmessage = (event: MessageEvent) => resolve(event.data as PidReport)
      channel.port2.start()
    })

    const reporter = createControlPidReporter(channel.port1, 3)
    reporter.report({ pid: 101, pgid: 101, label: "shell:echo hi" })

    expect(await received).toEqual({
      type: "pid-report",
      workerID: 3,
      entries: [{ pid: 101, pgid: 101, label: "shell:echo hi", workerID: 3 }],
    })
    channel.port1.close()
    channel.port2.close()
  })

  test("a worker's pid-report carries the hub-assigned pool id, not threadId", async () => {
    const channel = new MessageChannel()
    const received = new Promise<PidReport>((resolve) => {
      channel.port2.onmessage = (event: MessageEvent) => resolve(event.data as PidReport)
      channel.port2.start()
    })

    // The hub ships the pool slot id as `workerData.workerID`; the reporter must
    // prefer it over the isolate's own thread id (which is not the pool slot).
    const reporter = createControlPidReporter(channel.port1, reportedWorkerID({ workerID: 7 }, 999))
    reporter.report({ pid: 101, pgid: 101, label: "shell:echo hi" })

    expect(await received).toEqual({
      type: "pid-report",
      workerID: 7,
      entries: [{ pid: 101, pgid: 101, label: "shell:echo hi", workerID: 7 }],
    })
    channel.port1.close()
    channel.port2.close()
  })

  test("reportedWorkerID falls back to the isolate threadId only when no pool id was shipped", () => {
    expect(reportedWorkerID({ workerID: 7 }, 42)).toBe(7)
    expect(reportedWorkerID({}, 42)).toBe(42)
  })

  test("a throwing port emits a WARN pid report failed line with the reason and rethrows", () => {
    const records: Array<{ message: string; fields: Record<string, unknown> }> = []
    const log: WorkerLogger = (message, fields) => void records.push({ message, fields })
    const brokenPort = {
      postMessage: () => {
        throw new Error("port closed")
      },
    } as unknown as MessagePort

    const reporter = createControlPidReporter(brokenPort, 5, { log })
    expect(() => reporter.report({ pid: 101, pgid: 101, label: "shell:echo hi" })).toThrow("port closed")
    expect(records).toEqual([
      {
        message: workerLogMessage("pid report failed"),
        fields: { level: "warn", workerID: 5, reason: "port closed" },
      },
    ])
  })

  test("bootstrap forwards its log into the control pid reporter so E4 fires on a closed port", () => {
    // `bootstrapSessionWorker` builds the reporter over an open MessageChannel and
    // does not expose it, so the forwarding cannot be observed from outside. The
    // `pidReporterFactory` seam hands the test the reporter bootstrap built — and
    // the exact `log` it forwarded — so a throwing port can exercise E4 without a
    // real closed port (Bun silently accepts a post to a peer-closed port).
    const scope = {
      postMessage(_data: unknown, _transfer?: readonly Transferable[]) {},
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    const init: SessionWorkerInit = {
      snapshot: workerSnapshot(),
      env: { values: {} },
      model: {
        providerID: "mock",
        modelID: "m",
        apiId: "m",
        baseUrl: "http://127.0.0.1:1/v1",
        apiKey: "k",
        capabilities: { temperature: true, reasoning: false },
        limit: { context: 128_000, output: 4096 },
      },
      capabilities: { sessionWorkerPorts: true },
    }
    const records: Array<{ message: string; fields: Record<string, unknown> }> = []
    const log: WorkerLogger = (message, fields) => void records.push({ message, fields })
    const brokenPort = {
      postMessage: () => {
        throw new Error("port closed")
      },
    } as unknown as MessagePort

    let reporter: PidReporterInterface | undefined
    let reportedID: number | undefined
    bootstrapSessionWorker(init, scope, {
      log,
      pidReporterFactory: (_port, workerID, forwarded) => {
        reportedID = workerID
        reporter = createControlPidReporter(brokenPort, workerID, { log: forwarded })
        return reporter
      },
    })

    // The reporter exists and carries the forwarded log: reporting on the broken
    // port logs E4 (WARN) through it before rethrowing. The same `log` also carries
    // the E2 client connect lines, so assert on the E4 line specifically.
    expect(reporter).toBeDefined()
    expect(() => reporter!.report({ pid: 101, pgid: 101, label: "shell:echo hi" })).toThrow("port closed")
    const failed = records.filter((entry) => entry.message === workerLogMessage("pid report failed"))
    expect(failed).toEqual([
      {
        message: workerLogMessage("pid report failed"),
        fields: { level: "warn", workerID: reportedID, reason: "port closed" },
      },
    ])
  })

  test("a missing reporter (degraded boot) is a no-op; win32 reports nothing", () => {
    expect(() => reportDetachedShell(undefined, { pid: 7 }, "echo hi", "linux")).not.toThrow()

    const reports: Array<{ pid: number; pgid: number; label: string }> = []
    reportDetachedShell({ report: (entry) => void reports.push(entry) }, { pid: 7 }, "echo hi", "win32")
    expect(reports).toEqual([])
  })

  test("buildSessionWorkerLayer exposes the merged PidReporter at execution time (and none otherwise)", async () => {
    const withReporter = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
      pidReporter: { report: () => {} },
    })
    const without = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
    })

    const has = (layer: Layer.Layer<unknown, unknown>) =>
      Effect.runPromise(
        Effect.gen(function* () {
          return Option.isSome(yield* Effect.serviceOption(PidReporter))
        }).pipe(Effect.provide(layer)),
      )

    expect(await has(withReporter.layer)).toBe(true)
    expect(await has(without.layer)).toBe(false)
  })

  test.skipIf(process.platform === "win32")(
    "a spawned shell reports its PGID, which the registry reaps by worker id",
    async () => {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-pid-report-"))
      const reports: Array<{ pid: number; pgid: number; label: string }> = []
      const shellLayer = shellToolHarness(fixedPidSpawnerLayer(4321), {
        report: (entry) => void reports.push(entry),
      })

      try {
        await Effect.runPromise(
          Effect.gen(function* () {
            const shell = yield* ShellTool
            const tool = yield* shell.init()
            yield* tool.execute(
              { command: "echo hi" },
              {
                sessionID: sid("ses_pid"),
                messageID: MessageID.make("msg_pid"),
                callID: "",
                agent: "build",
                abort: AbortSignal.any([]),
                messages: [],
                metadata: () => Effect.void,
                ask: () => Effect.void,
              },
            )
          }).pipe(provideInstance(directory), Effect.provide(shellLayer)),
        )

        // The shell tool reported the detached child's PID as its PGID.
        expect(reports).toEqual([{ pid: 4321, pgid: 4321, label: "echo hi" }])

        // The report fills the hub mirror and `reap(workerID)` group-kills it.
        const registry = createPidRegistry({ reaper: { killGroup: () => {} } })
        registerReported(registry, {
          type: "pid-report",
          workerID: 1,
          entries: reports.map((entry) => ({ ...entry, workerID: 1 })),
        })
        expect(registry.has(4321)).toBe(true)
        expect(registry.reap(1, "SIGKILL")).toEqual([4321])
        expect(registry.size).toBe(0)
      } finally {
        fs.rmSync(directory, { recursive: true, force: true })
      }
    },
    30_000,
  )
})

// ---------------------------------------------------------------------------
// Step 28 — work-kind registry.
// ---------------------------------------------------------------------------

describe("createWorkRegistry (step 28)", () => {
  const marker: SessionWork = (sessionID) => Effect.succeed({ sessionID, value: { kind: "session" } })

  test("resolves a registered kind to its factory", () => {
    const registry = createWorkRegistry({ session: marker })
    expect(registry("session")).toBe(marker)
  })

  test("an unknown kind throws a typed UnknownWorkKindError", () => {
    const registry = createWorkRegistry({ session: marker })
    expect(() => registry("nope")).toThrow(UnknownWorkKindError)
    try {
      registry("nope")
    } catch (error) {
      expect((error as UnknownWorkKindError).kind).toBe("nope")
    }
  })

  test("the production registry runs session work for the session and legacy kinds", () => {
    const registry = productionWorkRegistry(marker)
    expect(registry("session")).toBe(marker)
    expect(registry(DEFAULT_WORK_KIND)).toBe(marker)
    expect(() => registry("other")).toThrow(UnknownWorkKindError)
  })
})

// ---------------------------------------------------------------------------
// Step 27 — real session work.
// ---------------------------------------------------------------------------

describe("createSessionWork (step 27)", () => {
  test("reads messages from the writer seam and returns the pipeline result", async () => {
    const built = buildLayer()
    const asked: string[] = []
    const work = createSessionWork({
      layer: built.layer,
      writer: stubClients().writer,
      threadId: 7,
      readMessages: (sessionID) => {
        asked.push(sessionID)
        return Effect.succeed([{ role: "user", content: "hello" }])
      },
    })

    const result = await Effect.runPromise(work(sid("ses_a")))
    expect(asked).toEqual([sid("ses_a")])
    expect(result.sessionID).toBe(sid("ses_a"))
    expect(result.value).toMatchObject({ text: "worker text", publishedSeq: 1, emitted: 1, threadId: 7 })
  })

  test("defaults to reading persisted messages through the writer client", async () => {
    const built = buildLayer()
    const queries: Array<{ method: string; sql: string; params?: readonly unknown[] }> = []
    const writer = {
      query: async (method: string, sql: string, params?: readonly unknown[]) => {
        queries.push({ method, sql, params })
        return [{ data: JSON.stringify({ role: "user", parts: [{ text: "from-db" }] }) }]
      },
      publish: async () => ({ seq: 1, committed: true }),
    }
    const work = createSessionWork({ layer: built.layer, writer, threadId: 0 })
    const result = await Effect.runPromise(work(sid("ses_b")))
    expect(queries.length).toBe(1)
    expect(queries[0].method).toBe("all")
    expect(queries[0].params).toEqual([sid("ses_b")])
    expect(result.value).toMatchObject({ text: "worker text" })
  })
})

// ---------------------------------------------------------------------------
// Worker EventV2 bridge wiring (spec Q6/Q13/Q15).
// ---------------------------------------------------------------------------

describe("worker event bridge wiring (Q6/Q13/Q15)", () => {
  const SessionThing = EventV2.define({
    type: "test.worker.session",
    schema: { sessionID: Schema.String, text: Schema.String },
  })

  test("the runnable graph is part of the worker and includes the bridge node", () => {
    const names = collectNodeNames(RUNNABLE_SESSION_NODES)
    expect(names.has("@opencode/session-worker/EventBridge")).toBe(true)
    expect(names.has("@opencode/EventV2Bridge")).toBe(true)
    expect(names.has("@opencode/Event")).toBe(true)
  })

  test("the production runnable graph carries the in-worker SessionProjector", () => {
    // Durable `message.updated` / `message.part.updated` events only become
    // `session`/`message`/`part` rows when the projector runs. Production must
    // include it; a test-added projector is no longer required.
    expect(collectNodeNames(RUNNABLE_SESSION_NODES).has("session-projector")).toBe(true)
  })

  test("the bridge forwards a worker-published session event to the egress sink verbatim", async () => {
    const forwarded: EventV2Event[] = []
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: {
        writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
        // The bridge's sink; the egress transport frames the verbatim event.
        egress: { emit: (event) => forwarded.push(event) },
        broker: { call: async () => null },
        backgroundJob: { wait: async () => null, cancel: async () => {} },
      },
      modelClient: modelLayer("worker text"),
    })

    await Effect.runPromise(
      Effect.gen(function* () {
        const events = yield* EventV2Bridge.Service
        yield* events.publish(SessionThing, { sessionID: "ses_1", text: "hi" })
      }).pipe(Effect.provide(built.layer)),
    )

    expect(forwarded).toHaveLength(1)
    expect(forwarded[0]).toMatchObject({
      type: "test.worker.session",
      properties: { sessionID: "ses_1", text: "hi" },
    })
    expect(forwarded[0].id).toStartWith("evt_")
  })

  test("bootstrap transfers the egress port the bridge sink rides on", () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    bootstrapSessionWorker(
      {
        snapshot: workerSnapshot(),
        env: { values: {} },
        model: {
          providerID: "mock",
          modelID: "m",
          apiId: "m",
          baseUrl: "http://127.0.0.1:1/v1",
          apiKey: "k",
          capabilities: { temperature: true, reasoning: false },
          limit: { context: 128_000, output: 4096 },
        },
        capabilities: { sessionWorkerPorts: true },
      },
      scope,
    )
    const ports = frames.find((frame) => (frame.data as { type?: string }).type === "ports")
    expect(ports).toBeDefined()
    expect((ports!.data as { egress?: unknown }).egress).toBeInstanceOf(MessagePort)
  })
})

describe("writerReadMessages", () => {
  test("extracts role + text parts and tolerates a read failure", async () => {
    const reader = writerReadMessages({
      query: async () => [
        { data: JSON.stringify({ role: "user", parts: [{ text: "a" }, { text: "b" }] }) },
        { data: { role: "assistant", content: "c" } },
        { data: "not-json" },
      ],
      publish: async () => ({ seq: 0, committed: false }),
    })
    expect(await Effect.runPromise(reader("s1"))).toEqual([
      { role: "user", content: "ab" },
      { role: "assistant", content: "c" },
    ])

    const failing = writerReadMessages({
      query: async () => {
        throw new Error("writer down")
      },
      publish: async () => ({ seq: 0, committed: false }),
    })
    expect(await Effect.runPromise(failing("s1"))).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// Flag-on status parity: the worker-local run-state reaches idle via the
// hub-proxy `SessionStatus`, the worker half of the flag-off cancel/completion
// transition. (The hub half — cancel after `hub.cancel` — lives in
// `hub-wiring.test.ts`.)
// ---------------------------------------------------------------------------

describe("worker run-state idle parity (flag-on)", () => {
  /**
   * The worker's `InstanceState`-backed services (e.g. `SessionRunState`) need an
   * ambient `InstanceRef`; a worker owns no project loader, so the snapshot
   * directory is both the working directory and the worktree.
   */
  const instance = {
    directory: workerSnapshot().directory,
    worktree: workerSnapshot().directory,
    project: undefined as never,
  }

  /** Build the worker graph with a `SessionStatus` recorded on the hub side. */
  const workerWithStatusStore = () => {
    const channel = new MessageChannel()
    const store = new Map<string, SessionStatus.Info>()
    serveHubProxy(
      {
        get: (sessionID) => store.get(sessionID as string) ?? { type: "idle" },
        set: (sessionID, status) => {
          store.set(sessionID as string, status as SessionStatus.Info)
        },
        list: () => [...store.entries()],
      },
      channel.port2,
    )
    const hubProxyClient = createHubProxyClient(channel.port1)
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
      hubProxies: { sessionStatus: hubProxyClient, permission: hubProxyClient, question: hubProxyClient },
    })
    return { built, store }
  }

  test("normal completion publishes session.status idle through the hub-proxy", async () => {
    const { built, store } = workerWithStatusStore()

    await Effect.runPromise(
      Effect.gen(function* () {
        const runState = yield* SessionRunState.Service
        yield* runState.ensureRunning(
          sid("ses_done"),
          Effect.succeed(withParts(sid("ses_done"))),
          Effect.succeed(withParts(sid("ses_done"))),
        )
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
    )

    // The worker-local runner's `onIdle` crossed the hub-proxy port, so the hub
    // observed the terminal idle — the same transition flag-off reaches through
    // its in-process runner's `onIdle`.
    expect(store.get("ses_done")).toEqual({ type: "idle" })
  })

  test("cancelling the worker-local runner also publishes idle through the hub-proxy", async () => {
    const { built, store } = workerWithStatusStore()

    await Effect.runPromise(
      Effect.gen(function* () {
        const runState = yield* SessionRunState.Service
        const started = yield* Latch.make()
        // A run that never settles keeps the runner busy until cancel interrupts it.
        const work = Effect.andThen(started.open, Effect.never)
        yield* runState
          .ensureRunning(sid("ses_run"), Effect.succeed(withParts(sid("ses_run"))), work)
          .pipe(Effect.forkChild)
        // Wait for the runner to be registered before cancelling it.
        yield* started.await
        yield* runState.cancel(sid("ses_run"))
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, instance)),
    )

    expect(store.get("ses_run")).toEqual({ type: "idle" })
  })
})

// ---------------------------------------------------------------------------
// End-to-end: the REAL `SessionPrompt.loop` inside the worker over the real
// writer-backed `Database`. This is the acceptance test for the worker DB fix:
// before it, the worker's raw-SQL `Database` had no Drizzle methods, so the
// loop died on `db.select`/`db.transaction` and wrote nothing.
//
// The production default graph (`RUNNABLE_SESSION_NODES`) now carries
// `SessionProjector`: `Session.updateMessage` / `updatePart` publish durable
// events, and the projector materializes the `session`/`message`/`part` rows the
// loop reads back — inside the same writer-pinned transaction as the event
// insert. The harness `createE2EContext` also names the projector explicitly, but
// that is the SAME node object, so it runs exactly once (verified).
// ---------------------------------------------------------------------------

const mkModelConfig = (provider: { baseUrl: string; apiKey: string; model: string }) => ({
  providerID: "mock",
  modelID: provider.model,
  apiId: provider.model,
  npm: "@ai-sdk/openai-compatible",
  baseUrl: provider.baseUrl,
  apiKey: provider.apiKey,
  headers: {},
})

/** Seed a session + one user message through the worker's own services. */
function seedSession(ctx: E2EContext, modelID: string, prompt: string) {
  return Effect.gen(function* () {
    const sessions = yield* Session.Service
    const session = yield* sessions.create({
      title: "e2e",
      agent: "build",
      permission: [{ permission: "*", pattern: "*", action: "allow" }],
    })
    const user = (yield* sessions.updateMessage({
      id: MessageID.ascending(),
      role: "user",
      sessionID: session.id,
      agent: "build",
      model: { providerID: "mock", modelID },
      time: { created: Date.now() },
    } as never)) as { id: MessageID }
    yield* sessions.updatePart({
      id: PartID.ascending(),
      messageID: user.id,
      sessionID: session.id,
      type: "text",
      text: prompt,
    } as never)
    return session.id
  }).pipe(Effect.provide(ctx.layer), Effect.provideService(InstanceRef, ctx.instance))
}

describe("real SessionPrompt.loop over the writer-backed worker Database (E2E)", () => {
  test("runs the loop against the mock provider and persists each durable row exactly once", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-"))
    await using provider = startMockProvider({ chunks: ["hel", "lo"] })
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      const result = await Effect.runPromise(work(sessionID))
      const value = result.value as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")

      // 1. Non-empty assistant text from the real streamed reply.
      expect(text).toBe("hello")
      expect(provider.requests.length).toBeGreaterThanOrEqual(1)

      // 2. Durable rows written through the writer, exactly once. `data` is
      // stored as JSON text by Drizzle, so parse it before inspecting.
      const json = (value: unknown): Record<string, unknown> =>
        typeof value === "string" ? (JSON.parse(value) as Record<string, unknown>) : (value as Record<string, unknown>)
      const messages = ctx.rows("message")
      expect(messages).toHaveLength(2)
      const roles = messages.map((row) => json(row.data).role)
      expect(roles.filter((role) => role === "assistant")).toHaveLength(1)
      expect(roles.filter((role) => role === "user")).toHaveLength(1)
      const texts = ctx
        .rows("part")
        .map((row) => json(row.data))
        .filter((data) => data.type === "text")
        .map((data) => data.text)
      expect(texts.filter((value) => value === "hello")).toHaveLength(1)
      expect(ctx.rows("event").length).toBeGreaterThan(0)

      // 3. session.status busy -> idle (the worker half of completion parity).
      const statuses = ctx.statusLog.filter((entry) => entry.sessionID === sessionID).map((entry) => entry.status)
      expect(statuses[0]).toBe("busy")
      expect(statuses[statuses.length - 1]).toBe("idle")

      // 4. Every durable write serialized through the single writer.
      expect(ctx.writerOps.length).toBeGreaterThan(0)
      expect(ctx.writerOps.some((op) => op.startsWith("tx:") || op === "begin" || op === "commit")).toBe(true)
      const eventIDs = ctx.rows("event").map((row) => row.id)
      expect(new Set(eventIDs).size).toBe(eventIDs.length)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)

  test("a git-backed project completes the loop without UnimplementedError", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-git-"))
    await using provider = startMockProvider({ chunks: ["hel", "lo"] })
    const config = mkModelConfig(provider)
    // A git-backed project: without the worker Snapshot boundary the processor's
    // `snapshot.track()` reaches `git` through the hub-owned `AppProcess` and the
    // run dies with `UnimplementedError`.
    const snapshot = workerSnapshot({ directory, projectId: "prj_git", projectVcs: "git" })
    const ctx = createE2EContext({
      snapshot,
      modelConfig: config,
      replacements: workerGraphReplacements(config),
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: workerInstanceContext(snapshot),
      })
      const result = await Effect.runPromise(work(sessionID))
      const value = result.value as { parts: ReadonlyArray<{ type: string; text?: string }> }
      const text = value.parts
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("")
      expect(text).toBe("hello")
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)

  test("a session whose model differs from the serialized worker model fails with a typed error before any provider call", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-mismatch-"))
    await using provider = startMockProvider({ chunks: ["never"] })
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
    })
    try {
      // The worker was bootstrapped with `provider.model`; the session asks for a
      // different model, which the single-model reduced provider cannot serve.
      const sessionID = await Effect.runPromise(seedSession(ctx, "other-model", "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      const error = await Effect.runPromise(work(sessionID)).then(
        () => undefined,
        (cause: unknown) => cause,
      )

      // Typed, names both models, and diagnosed before the provider is called.
      expect(error).toBeInstanceOf(WorkerModelMismatchError)
      expect((error as Error).message).toContain("other-model")
      expect((error as Error).message).toContain(provider.model)
      expect(provider.requests).toHaveLength(0)
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

// ---------------------------------------------------------------------------
// End-to-end: a real worker `SessionPrompt.loop` run that ERRORS. The processor
// halts on the provider failure and publishes a NON-durable `session.error`,
// which the worker's EventV2→egress bridge forwards verbatim across the isolate
// to the hub-side sink. The audit (`.parallel-implement/o1-w4-audit.md`, phase 5)
// flagged that `session.error` was only unit-covered at the hub sink, never over
// a real worker error run.
//
// The verbatim shape is the flag-OFF EventV2 wire shape `{ id, type, properties }`
// (`properties` is the event `data`), NOT the writer-style `{ id, type, data }`.
// ---------------------------------------------------------------------------

describe("real SessionPrompt.loop error over the worker (E2E)", () => {
  test("a provider failure crosses the isolate as one verbatim session.error and persists one error row", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-err-"))
    // A non-retryable status (400) makes the processor halt on the first failure
    // instead of backing off through the 5xx retry ladder (~60s).
    await using provider = startMockProvider({
      status: 400,
      errorBody: { error: { message: "bad request from mock provider" } },
    })

    // Observe the hub-side events-port sink: the worker's egress handle IS that
    // sink, so replacing the egress client node captures exactly what crosses.
    const crossed: EventV2Event[] = []
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
      replacements: [
        [EventEgressClientNode, Layer.succeed(EventEgressClientService, { emit: (event) => void crossed.push(event) })],
      ],
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
      })
      await Effect.runPromise(work(sessionID))

      // 1. The error crossed the isolate exactly once, verbatim.
      const errors = crossed.filter((event) => event.type === "session.error")
      expect(errors).toHaveLength(1)
      const error = errors[0]
      expect(Object.keys(error).sort()).toEqual(["id", "properties", "type"])
      expect(error.id).toStartWith("evt_")
      const properties = error.properties as { sessionID?: string; error?: { name?: string; message?: string } }
      expect(properties.sessionID).toBe(sessionID)
      expect(properties.error).toBeDefined()

      // The hub sink accepts that same verbatim event and republishes it
      // live-only — closing the "real run -> hub sink" gap the audit flagged.
      const republished: Array<{ id?: string; type: string; data: unknown }> = []
      createHubEventSink({
        publisher: {
          publish: (event) => void republished.push(event),
          publishDefinition: (definition, data) => void republished.push({ type: definition.type, data }),
        },
      }).event(error)
      expect(republished).toEqual([{ id: error.id, type: "session.error", data: properties }])

      // 2. The durable error row is persisted exactly once: one assistant message
      //    carrying the error, and no duplicate of it.
      const json = (value: unknown): Record<string, unknown> =>
        typeof value === "string" ? (JSON.parse(value) as Record<string, unknown>) : (value as Record<string, unknown>)
      const assistants = ctx
        .rows("message")
        .map((row) => json(row.data))
        .filter((data) => data.role === "assistant")
      expect(assistants).toHaveLength(1)
      expect(assistants[0].error).toBeDefined()
    } finally {
      await ctx.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 60_000)
})

// ---------------------------------------------------------------------------
// Fix B — the worker's ambient `InstanceRef` must carry a real project.
//
// Production used to install `project: undefined as never`, so every
// `ctx.project.id` reader (`Snapshot.state`, `Session.create`,
// `event-v2-bridge`) threw `TypeError` as soon as the worker booted. The hub now
// ships the project identity on the snapshot, and the entry rebuilds a valid
// `InstanceContext.project` from it.
// ---------------------------------------------------------------------------

describe("workerInstanceContext (Fix B)", () => {
  test("rebuilds a valid project from the snapshot identity", () => {
    const snapshot = workerSnapshot({
      directory: "/tmp/oc-worker-dir",
      projectId: "prj_real",
      projectVcs: "git",
      worktree: "/tmp/oc-worker-wt",
    })
    const ctx = workerInstanceContext(snapshot)
    expect(ctx.directory).toBe("/tmp/oc-worker-dir")
    expect(ctx.worktree).toBe("/tmp/oc-worker-wt")
    expect(String(ctx.project.id)).toBe("prj_real")
    expect(ctx.project.vcs).toBe("git")
    expect(ctx.project.worktree).toBe("/tmp/oc-worker-wt")
  })

  test("a snapshot without a project id falls back to the directory, never undefined", () => {
    const ctx = workerInstanceContext(workerSnapshot({ directory: "/tmp/oc-worker-bare" }))
    expect(String(ctx.project.id)).toBe("/tmp/oc-worker-bare")
    expect(ctx.project.vcs).toBeUndefined()
    expect(ctx.worktree).toBe("/tmp/oc-worker-bare")
  })
})

describe("worker snapshot tracking without vcs (Fix B)", () => {
  test("Snapshot.track is a safe no-op when the project has no vcs", async () => {
    // A git project would make `track` shell out to the (worker-mocked)
    // `AppProcess`; a vcs-less project must short-circuit before any git call.
    const snapshot = workerSnapshot({ directory: "/tmp/oc-worker-novcs", projectId: "prj_novcs" })
    const built = buildSessionWorkerLayer({
      snapshot,
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
    })
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const snap = yield* Snapshot.Service
        return yield* snap.track()
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, workerInstanceContext(snapshot))),
    )
    expect(result).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// Wave-2 consolidation #2: a git-backed project must not crash the worker.
//
// The worker has no OS child-process authority (`AppProcess` is a hub-owned
// node), so the real `Snapshot` service dies with `UnimplementedError` the
// moment it shells out to `git`. The worker installs a fail-soft `Snapshot`
// boundary instead; the session still completes, only snapshot/revert is
// unavailable in the isolate.
// ---------------------------------------------------------------------------

describe("worker Snapshot boundary for a git-backed project (Wave-2 consolidation #2)", () => {
  const gitConfig: WorkerModelConfig = {
    providerID: "mock",
    modelID: "mock-model",
    apiId: "mock-model",
    npm: "@ai-sdk/openai-compatible",
    baseUrl: "http://127.0.0.1:1/v1",
    apiKey: "k",
    headers: {},
  }

  test("Snapshot.track is a fail-soft no-op when the project is git-backed", async () => {
    const snapshot = workerSnapshot({ directory: "/tmp/oc-worker-git", projectId: "prj_git", projectVcs: "git" })
    const built = buildSessionWorkerLayer({
      snapshot,
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
      replacements: workerGraphReplacements(gitConfig),
    })
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const snap = yield* Snapshot.Service
        return yield* snap.track()
      }).pipe(Effect.provide(built.layer), Effect.provideService(InstanceRef, workerInstanceContext(snapshot))),
    )
    expect(result).toBeUndefined()
  })
})

describe("worker event bridge tolerates a project-less instance (Fix B guard)", () => {
  const ProjectlessThing = EventV2.define({
    type: "test.worker.projectless",
    schema: { sessionID: Schema.String, text: Schema.String },
  })

  test("the GlobalBus listener does not throw when the ambient instance has no project", async () => {
    const received: Array<{ type?: string; project?: string }> = []
    const onEvent = (event: { payload?: { type?: string }; project?: string }) =>
      void received.push({ type: event.payload?.type, project: event.project })
    GlobalBus.on("event", onEvent as never)
    const built = buildSessionWorkerLayer({
      snapshot: workerSnapshot(),
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
    })
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const events = yield* EventV2Bridge.Service
          yield* events.publish(ProjectlessThing, { sessionID: "ses_x", text: "hi" })
        }).pipe(
          Effect.provide(built.layer),
          Effect.provideService(InstanceRef, {
            directory: workerSnapshot().directory,
            worktree: workerSnapshot().directory,
            project: undefined as never,
          }),
        ),
      )
      const forwarded = received.filter((event) => event.type === "test.worker.projectless")
      expect(forwarded).toHaveLength(1)
      expect(forwarded[0].project).toBeUndefined()
    } finally {
      GlobalBus.off("event", onEvent as never)
    }
  })
})

// ---------------------------------------------------------------------------
// Phase 1 — the worker receives the REAL model descriptor, not a bare identity.
// ---------------------------------------------------------------------------

describe("worker model descriptor", () => {
  const descriptorModel: SessionWorkerModel = {
    providerID: "fixture",
    modelID: "fixture-model",
    apiId: "fixture-api-id",
    npm: "@ai-sdk/anthropic",
    baseUrl: "https://fixture.example/v1",
    apiKey: "sk-fixture",
    headers: { "x-provider": "1" },
    capabilities: {
      temperature: false,
      reasoning: true,
      interleaved: true,
      toolcall: true,
      attachment: false,
    },
    limit: { context: 200_000, output: 32_000 },
    options: { temperature: 0.2, topP: 0.9 },
    variants: { high: { reasoningEffort: "high" } },
    providerOptions: { includeUsage: true, useCompletionUrls: true },
  }

  test("the full descriptor survives a structuredClone round-trip unchanged", () => {
    expect(assertStructuredCloneable(descriptorModel)).toEqual(descriptorModel)
  })

  test("parseSessionWorkerInit accepts a descriptor payload", () => {
    const parsed = parseSessionWorkerInit({
      snapshot: workerSnapshot(),
      env: { values: {} },
      model: descriptorModel,
    })

    expect(parsed.model).toEqual(descriptorModel)
    expect(parsed.model.limit.output).toBe(32_000)
    expect(parsed.model.capabilities.temperature).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// Phase 4 + 6.3 — cancelling a worker-local session must STOP the real
// `SessionPrompt.loop` (the worker-local `SessionRunState` runner, table B), not
// just the executor's outer machine (table A). The old cancel path interrupted
// the outer fiber while table B's `runLoop` survived; the run control seam routes
// cancel/assertNotBusy to table B.
// ---------------------------------------------------------------------------

/** Poll a predicate until it is true, failing loud on timeout. */
async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  message: string,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`timed out: ${message}`)
}

describe("real SessionPrompt.loop cancellation (Phase 4 + 6.3)", () => {
  /**
   * An OpenAI-compatible SSE provider that emits a partial assistant turn and
   * then never closes the response, so the loop stays blocked inside the stream
   * until it is cancelled.
   */
  function startHangingProvider() {
    const encoder = new TextEncoder()
    const requests: Array<Record<string, unknown>> = []
    const frame = (delta: Record<string, unknown>): string =>
      "data: " + JSON.stringify({ id: "chatcmpl-hang", object: "chat.completion.chunk", choices: [{ delta }] }) + "\n\n"
    const server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      async fetch(request) {
        const url = new URL(request.url)
        if (request.method !== "POST" || !url.pathname.endsWith("/chat/completions")) {
          return new Response("not found", { status: 404 })
        }
        requests.push((await request.json().catch(() => ({}))) as Record<string, unknown>)
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encoder.encode(frame({ role: "assistant" })))
            controller.enqueue(encoder.encode(frame({ content: "partial" })))
            // Intentionally never closes: the run blocks until cancelled.
          },
        })
        return new Response(stream, { headers: { "content-type": "text/event-stream" } })
      },
    })
    return {
      baseUrl: `http://127.0.0.1:${server.port}/v1`,
      apiKey: "k",
      model: "test-model",
      requests,
      stop: () => server.stop(true),
    }
  }

  test("cancel stops the loop, finalizes the assistant, and run-state goes idle", async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-e2e-cancel-"))
    const provider = startHangingProvider()
    const ctx = createE2EContext({
      snapshot: workerSnapshot({ directory }),
      modelConfig: mkModelConfig(provider),
    })
    try {
      const sessionID = await Effect.runPromise(seedSession(ctx, provider.model, "hi"))
      let control: SessionRunControl | undefined
      const work = createSessionWork({
        layer: ctx.layer,
        writer: ctx.writer,
        threadId: 7,
        prompt: true,
        instance: ctx.instance,
        registerControl: (registered) => {
          control = registered
        },
      })

      const running = Effect.runPromise(work(sessionID))
      await waitUntil(() => control?.busy(sessionID) ?? false, "worker run-state never became busy")
      await waitUntil(() => provider.requests.length >= 1, "provider never received the request")
      const requestsBeforeCancel = provider.requests.length

      await control!.cancel(sessionID)

      // 6.3: the run-state reflects the REAL loop after cancel, not a stale table A.
      expect(await control!.busy(sessionID)).toBe(false)

      // The cancelled run settles; it must not hang.
      const outcome = await Promise.race([
        running.then(
          () => "resolved" as const,
          () => "rejected" as const,
        ),
        new Promise<"hung">((resolve) => setTimeout(() => resolve("hung"), 3000)),
      ])
      expect(outcome).not.toBe("hung")

      // No further provider request after cancel (flag-OFF parity).
      expect(provider.requests.length).toBe(requestsBeforeCancel)

      // The assistant row is finalized with an AbortedError and a completion time.
      const json = (value: unknown): Record<string, unknown> =>
        typeof value === "string" ? (JSON.parse(value) as Record<string, unknown>) : (value as Record<string, unknown>)
      const assistants = ctx
        .rows("message")
        .map((row) => json(row.data))
        .filter((data) => data.role === "assistant")
      expect(assistants).toHaveLength(1)
      const error = assistants[0].error as { name?: string } | undefined
      expect(error?.name).toBe("MessageAbortedError")
      expect(assistants[0].time).toBeDefined()
      expect((assistants[0].time as { completed?: number }).completed).toBeDefined()
    } finally {
      await ctx.dispose()
      provider.stop()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }, 30_000)
})

// ---------------------------------------------------------------------------
// Task 4 (Wave 1 follow-up) — the degraded (no hub-port) `ModelClient` path must
// run the worker-hosted plugin hooks, not the no-op pass-through, so a plugin's
// `chat.params` / `chat.headers` mutations reach the reduced LLM.
// ---------------------------------------------------------------------------

describe("degraded ModelClient worker plugin host (task 4)", () => {
  /** Run one degraded-boot session turn through the installed executor. */
  async function runDegradedTurn(init: SessionWorkerInit): Promise<{ readonly type: string }> {
    const frames: Array<{ type?: string }> = []
    const scope = {
      postMessage(data: unknown) {
        frames.push(data as { type?: string })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    bootstrapSessionWorker(init, scope)
    scope.onmessage!({ data: { type: "run", id: 1, sessionID: "ses_plugin", kind: "session" } })
    await waitUntil(
      () => frames.some((frame) => frame.type === "done" || frame.type === "error"),
      "executor never settled",
    )
    return frames.find((frame) => frame.type === "done" || frame.type === "error") as { type: string }
  }

  const degradedInit = (
    provider: { baseUrl: string; apiKey: string; model: string },
    plugins?: readonly WorkerPlugin[],
  ): SessionWorkerInit => ({
    snapshot: workerSnapshot(),
    env: { values: {} },
    model: {
      providerID: "mock",
      modelID: provider.model,
      apiId: provider.model,
      npm: "@ai-sdk/openai-compatible",
      baseUrl: provider.baseUrl,
      apiKey: provider.apiKey,
      capabilities: { temperature: true, reasoning: false },
      limit: { context: 128_000, output: 4096 },
    },
    ...(plugins === undefined ? {} : { plugins }),
  })

  test("applies a worker-hosted chat.params hook in the reduced model path", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const outcome = await runDegradedTurn(
      degradedInit(provider, [
        {
          name: "sampling",
          hooks: {
            "chat.params": (_input, output) => {
              output.temperature = 0.25
            },
          },
        },
      ]),
    )

    expect(outcome.type).toBe("done")
    expect(provider.requests[0].body.temperature).toBe(0.25)
  })

  test("uses the no-op Plugin when no hooks are shipped", async () => {
    await using provider = startMockProvider({ text: "ok" })
    const outcome = await runDegradedTurn(degradedInit(provider))

    expect(outcome.type).toBe("done")
    // Without shipped hooks the reduced path uses the no-op Plugin, so the
    // plugin's mutation is absent.
    expect(provider.requests[0].body.temperature).not.toBe(0.25)
  })
})
