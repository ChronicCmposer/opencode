// Session-worker runtime tests.
//
// The production entry is import-safe outside a worker (it only bootstraps when
// `workerData` is present), so the pure aggregation / registry / work builders
// are exercised in-process here.

import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test"
import { Database } from "bun:sqlite"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { DateTime, Effect, Exit, Latch, Layer, Option, Schema, Sink, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { Flag } from "@opencode-ai/core/flag/flag"
import { openConnection } from "@opencode-ai/core/database/connection"
import { Database as DatabaseService } from "@opencode-ai/core/database/database"
import { AppProcess } from "@opencode-ai/core/process"
import { RipgrepBinary } from "@opencode-ai/core/ripgrep/binary"
import { workerAppProcessLayer } from "@/session/worker/app-process-report"
import { ShellTool } from "@/tool/shell"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { EventV2 } from "@opencode-ai/core/event"
import { Event } from "@opencode-ai/schema/event"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { EventV2Bridge } from "@/event-v2-bridge"
import { MessageID, PartID, SessionID } from "@/session/schema"
import { BackgroundJob } from "@/background/job"
import { BackgroundJob as CoreBackgroundJob } from "@opencode-ai/core/background-job"
import { Session } from "@/session/session"
import { SessionPrompt, type PromptInput } from "@/session/prompt"
import { SessionRunState } from "@/session/run-state"
import { SessionStatus } from "@/session/status"
import { InstanceRef } from "@/effect/instance-ref"
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
  type SessionWorkFactory,
} from "@/session/worker/execute"
import {
  MissingWorkerPortError,
  bootstrapSessionWorker,
  buildWorkerClients,
  createBackgroundWorkProbe,
  createControlPidReporter,
  createSessionWork,
  createWorkerDatabaseWriter,
  createWorkRegistry,
  hasRunningBackgroundJob,
  parseSessionWorkerInit,
  productionWorkRegistry,
  reportedWorkerID,
  workerGraphReplacements,
  workerInstanceContext,
  writerReadMessages,
  type SessionWorkerInit,
  type SessionWorkerModel,
  type WorkerWriterClient,
} from "@/session/worker/session-worker"
import { workerLogMessage } from "@/session/worker/worker-logging"
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
import { Snapshot } from "@/snapshot"
import { GlobalBus } from "@/bus/global"
import { createHubEventSink } from "@/session/worker/hub"
import { recordingLogger } from "../../lib/recorders"
import { workerSnapshot } from "../../lib/snapshot"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import { markSchemaIdentity } from "@/session/worker/db"
import { withParts } from "../../lib/session-parts"
import { startMockProvider } from "../../lib/mock-provider"
import { createE2EContext, type E2EContext } from "../../lib/parity-harness"
import { assertStructuredCloneable } from "../../lib/session-worker-init"
import {
  DEFAULT_TEST_TIMEOUT_MS,
  REAL_WORKER_SLOW_TEST_TIMEOUT_MS,
} from "../../lib/worker-timeouts"

// Bun resets the per-file default to 5s; this file boots real worker isolates
// and publishes 17MB blobs, which can exceed 5s on a busy host.
setDefaultTimeout(DEFAULT_TEST_TIMEOUT_MS)

const sid = (value: string) => SessionID.make(value)

// Full-port bootstraps open the worker's per-worker WAL connection at
// `Database.path()`. Point that at a migrated file database for this file, so a
// full boot exercises the schema gate instead of failing on the suite's
// `:memory:` default (which cannot be migrated across connections).
const previousDatabasePath = Flag.OPENCODE_DB
let workerDatabaseDirectory: string | undefined
beforeAll(() => {
  workerDatabaseDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-session-worker-db-"))
  const filename = path.join(workerDatabaseDirectory, "opencode.db")
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
  Flag.OPENCODE_DB = filename
})
afterAll(() => {
  Flag.OPENCODE_DB = previousDatabasePath
  if (workerDatabaseDirectory !== undefined) fs.rmSync(workerDatabaseDirectory, { recursive: true, force: true })
})

const stubClients = (): SessionLayerOptions["clients"] => ({
  writer: { query: async () => [], publish: async () => ({ seq: 1, committed: true }) },
  egress: { emit: () => {} },
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

describe("worker background-work probe", () => {
  const job = (overrides: Partial<BackgroundJob.Info> & Pick<BackgroundJob.Info, "id">): BackgroundJob.Info => ({
    type: "task",
    status: "running",
    started_at: 1,
    ...overrides,
  })

  test("a running job for the session keeps the worker alive", () => {
    expect(hasRunningBackgroundJob([job({ id: "job_1", parentID: "ses_1" })], "ses_1")).toBe(true)
  })

  test("a running unparented job counts (it still owns this isolate)", () => {
    expect(hasRunningBackgroundJob([job({ id: "job_1" })], "ses_1")).toBe(true)
  })

  test("a job for another session does not keep this worker alive", () => {
    expect(hasRunningBackgroundJob([job({ id: "job_1", parentID: "ses_2" })], "ses_1")).toBe(false)
  })

  test("terminal jobs do not keep the worker alive", () => {
    for (const status of ["completed", "error", "cancelled"] as const) {
      expect(hasRunningBackgroundJob([job({ id: "job_1", parentID: "ses_1", status })], "ses_1")).toBe(false)
    }
    expect(hasRunningBackgroundJob([], "ses_1")).toBe(false)
  })

  test("the probe follows a real process-local job registry from running to terminal", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const jobs = yield* CoreBackgroundJob.make
          const probe = createBackgroundWorkProbe(jobs)
          const sessionID = sid("ses_probe")
          const active = () => Effect.promise(() => probe(sessionID))

          expect(yield* active()).toBe(false)
          yield* jobs.start({ id: "job_1", type: "task", parentID: sessionID, run: Effect.never })
          expect(yield* active()).toBe(true)

          yield* jobs.cancel("job_1")
          expect(yield* active()).toBe(false)
        }),
      ),
    )
  })
})

describe("bootstrapSessionWorker control-port transfer", () => {
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
    role: "session" as const,
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

  test("transfers worker and hub-proxy ports when the capability is advertised", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    const types = frameTypes(frames)
    // The egress bundle and the hub-proxy control port are both transferred for
    // a full hub; the writer has no port (D5).
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

  test("the bootstrap handle exposes only disposal, not unread plugin-tool seams", () => {
    const { scope } = fakeScope()
    const handle = bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)
    expect(Object.keys(handle)).toEqual(["dispose"])
  })

  test("a shutdown queued before boot is acknowledged before the DB/executor opens", () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
      // Deliver the shutdown synchronously the moment the worker registers its
      // listener. The listener must be registered before the DB opens, so this
      // aborts the boot instead of losing the frame (or opening the WAL handle
      // only to have the isolate terminated under it).
      addEventListener(_type: "message", listener: (event: { data: unknown }) => void) {
        listener({ data: { type: "shutdown", id: 7 } })
      },
      removeEventListener() {},
    }

    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope)

    const types = frameTypes(frames)
    expect(types).toContain("shutdown-ack")
    expect(types).not.toContain("ports")
    expect(types).not.toContain("ready")
    expect(frames.find((frame) => (frame.data as { type?: string }).type === "shutdown-ack")?.data).toEqual({
      type: "shutdown-ack",
      id: 7,
    })
  })

  test("an orderly shutdown closes a pinned write transaction, a throwing disposal, and still acks", async () => {
    const frames: Array<{ data: unknown; transfer: readonly Transferable[] }> = []
    let listener: ((event: { data: unknown }) => void) | undefined
    const scope = {
      postMessage(data: unknown, transfer?: readonly Transferable[]) {
        frames.push({ data, transfer: transfer ?? [] })
      },
      onmessage: null as ((event: { data: unknown }) => void) | null,
      addEventListener(_type: "message", next: (event: { data: unknown }) => void) {
        listener = next
      },
      removeEventListener() {},
    }
    let writer: WorkerWriterClient | undefined
    const bootstrap = bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope, {
      observeWriter: (handle) => {
        writer = handle
      },
      // A client whose disposal throws must not skip the WAL close or the ack.
      observeHubProxy: (client) => {
        client.dispose = () => {
          throw new Error("hub proxy dispose boom")
        }
      },
    })
    try {
      expect(writer).toBeDefined()
      // Pin a real write transaction on the worker's own WAL connection. The
      // writer seam reads through the worker's scoped connection; transaction
      // control belongs to the connection, not a deleted writer API.
      await writer!.query("exec", "BEGIN IMMEDIATE")
      listener?.({ data: { type: "shutdown", id: 21 } })
      expect(frameTypes(frames)).toContain("shutdown-ack")
      expect(frames.find((frame) => (frame.data as { type?: string }).type === "shutdown-ack")?.data).toEqual({
        type: "shutdown-ack",
        id: 21,
      })
      // The close rolled the pinned transaction back and released the handle, so
      // a fresh connection can take the write lock without SQLITE_BUSY.
      const probe = new Database(Flag.OPENCODE_DB as string)
      try {
        probe.exec("PRAGMA busy_timeout = 0")
        expect(() => probe.exec("BEGIN IMMEDIATE")).not.toThrow()
        probe.exec("ROLLBACK")
      } finally {
        probe.close()
      }
    } finally {
      for (const frame of frames) {
        for (const value of Object.values(frame.data as Record<string, unknown>)) {
          if (value instanceof MessagePort) value.close()
        }
      }
      bootstrap.dispose()
    }
  })

  test("degrades to the reduced pipeline without the capability", () => {
    const { frames, scope } = fakeScope()
    bootstrapSessionWorker(init(), scope)
    const types = frameTypes(frames)
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
    // egress + egressControl are transferred; there is no writer or broker port.
    expect(ports!.transfer).toHaveLength(2)
  })

  test("dispose closes the installed executor (its scope finalizers run)", () => {
    const { scope } = fakeScope()
    let disposed = 0
    const handle = bootstrapSessionWorker(init(), scope, {
      installExecutor: () => ({
        handle: async () => ({ type: "pong", id: 0 }),
        dispose: async () => {
          disposed += 1
        },
      }),
    })

    expect(disposed).toBe(0)
    handle.dispose()
    expect(disposed).toBe(1)
  })

  test("dispose tears the executor down before closing the worker DB", async () => {
    const { scope } = fakeScope()
    let releaseExecutor!: () => void
    const executorGate = new Promise<void>((resolve) => {
      releaseExecutor = resolve
    })
    let writerDisposed = false
    const handle = bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope, {
      observeWriter: (writer) => {
        const dispose = writer.dispose.bind(writer)
        writer.dispose = () => {
          writerDisposed = true
          dispose()
        }
      },
      installExecutor: () => ({
        handle: async () => ({ type: "pong", id: 0 }),
        dispose: async () => {
          await executorGate
        },
      }),
    })

    const disposing = handle.dispose()
    await new Promise((resolve) => setTimeout(resolve, 10))
    // The executor still owns its scope, so the DB/writer must not be closed yet.
    expect(writerDisposed).toBe(false)

    releaseExecutor()
    await disposing
    expect(writerDisposed).toBe(true)
  })

  test("dispose rejects pending hub-proxy calls and removes egress listeners", async () => {
    const { scope } = fakeScope()
    const recorder = recordingLogger()
    let client: import("@/session/worker/hub-proxy-wire").HubProxyClient | undefined
    const handle = bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope, {
      bridge: recorder.bridge,
      observeHubProxy: (observed) => {
        client = observed
      },
    })
    expect(client).toBeDefined()

    const call = client!.call("sessionStatus.get", []).then(
      () => "resolved",
      () => "rejected",
    )
    const stillPending = await Promise.race([
      call,
      new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 50)),
    ])
    expect(stillPending).toBe("pending")

    handle.dispose()

    const settled = await Promise.race([
      call,
      new Promise<string>((resolve) => setTimeout(() => resolve("pending"), 200)),
    ])
    expect(settled).toBe("rejected")
    expect(recorder.lines.map((entry) => entry.message)).toContain(workerLogMessage("egress closed"))
  })

  test("wires a real logger into the client lifecycles (not the noop default)", () => {
    const { scope } = fakeScope()
    const recorder = recordingLogger()
    bootstrapSessionWorker(init({ sessionWorkerPorts: true }), scope, { bridge: recorder.bridge })
    const messages = recorder.lines.map((entry) => entry.message)
    // Each port-backed peer emits its connected line at construction; without
    // a real logger these were dormant in production.
    expect(messages).toContain(workerLogMessage("egress connected"))
  })

  test("a cancel with no active run does not latch into the next pipeline run", async () => {
    const { scope } = fakeScope()
    let control: SessionRunControl | undefined
    let work: SessionWorkFactory | undefined
    const handle = bootstrapSessionWorker(init(), scope, {
      installExecutor: (_scope, options) => {
        control = options.control
        work = options.work
        return {
          handle: async () => ({ type: "pong", id: 0 }),
          dispose: async () => {},
        }
      },
    })
    try {
      const sessionID = sid("ses_pipeline_idle_cancel")
      // Pipeline-only graphs never register `SessionRunState`, so the control is
      // permanently absent. An idle cancel must not outlive the (non-existent)
      // run it was meant for.
      expect(control).toBeDefined()
      expect(work).toBeDefined()
      await control!.cancel(sessionID)

      const exit = await Effect.runPromise(Effect.exit(work!(DEFAULT_WORK_KIND)(sessionID)))
      expect(Exit.hasInterrupts(exit)).toBe(false)
    } finally {
      await handle.dispose()
    }
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
    role: "session" as const,
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
})

// ---------------------------------------------------------------------------
// Client aggregation.
// ---------------------------------------------------------------------------

describe("buildWorkerClients", () => {
  const emptyChannels = () => ({
    egress: new MessageChannel(),
  })

  test("the egress port is required; writer and broker are worker-local/absent", () => {
    expect(() => buildWorkerClients({})).toThrow(MissingWorkerPortError)
    expect(() => buildWorkerClients({})).toThrow(/egress/)

    // D5: there is no writer port/RPC and no broker. The writer is opened
    // over the worker's own WAL connection once the egress relay is present.
    const channels = emptyChannels()
    const clients = buildWorkerClients({ egress: channels.egress.port1 })
    try {
      // Background jobs are process-local, so the worker client bundle has no
      // idle job handle at all.
      expect("backgroundJob" in clients).toBe(false)
      expect(typeof clients.writer.publish).toBe("function")
      expect(typeof clients.writer.query).toBe("function")
      expect(typeof clients.writer.dispose).toBe("function")
      // D5/S4: the legacy writer transaction API is deleted; the worker database
      // owns transaction control and the writer only publishes/reads/disposes.
      expect("begin" in clients.writer).toBe(false)
      expect("txQuery" in clients.writer).toBe(false)
      expect("commit" in clients.writer).toBe(false)
      expect("rollback" in clients.writer).toBe(false)
    } finally {
      clients.writer.dispose()
    }
  })

  test("wires the egress control port into the mesh client", () => {
    const channels = emptyChannels()
    const control = new MessageChannel()
    const clients = buildWorkerClients({
      egress: channels.egress.port1,
      egressControl: control.port1,
    })
    try {
      // The dedicated control port gives the egress client a live mesh (durable
      // ack/resync); without it `mesh` is `undefined` and the control plane dies.
      expect(clients.egress.mesh).toBeDefined()
    } finally {
      clients.writer.dispose()
    }
  })

  test("omits the mesh client when no control port is supplied", () => {
    const channels = emptyChannels()
    const clients = buildWorkerClients({ egress: channels.egress.port1 })
    try {
      expect(clients.egress.mesh).toBeUndefined()
    } finally {
      clients.writer.dispose()
    }
  })

  test("forwards a supplied log into the egress lifecycle", () => {
    const channels = emptyChannels()
    const recorder = recordingLogger()
    const clients = buildWorkerClients(
      { egress: channels.egress.port1 },
      { workerID: 4, bridge: recorder.bridge },
    )
    try {
      const connected = recorder.lines.filter((entry) => entry.message.endsWith("connected"))
      expect(connected.map((entry) => entry.message)).toEqual([workerLogMessage("egress connected")])
      for (const entry of connected) expect(entry.fields.workerID).toBe(4)
    } finally {
      clients.writer.dispose()
    }
  })
})

describe("worker database writer blob directory (D16)", () => {
  const entries = (directory: string): string[] =>
    fs.existsSync(directory) ? fs.readdirSync(directory).sort() : []

  test("a custom-file worker publishes and removes oversized blobs in its own event-blobs", async () => {
    // The fallback worker writer must derive D16 from the file it opened, never
    // the global database. `Flag.OPENCODE_DB` is the suite's migrated global DB.
    const globalBlobDir = path.join(path.dirname(Flag.OPENCODE_DB as string), "event-blobs")
    const globalBefore = entries(globalBlobDir)
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "oc-worker-blob-dir-"))
    const filename = path.join(directory, "custom.db")
    const setup = openConnection({ filename })
    try {
      for (const statement of migrationStatements) setup.exec(statement)
      markSchemaIdentity(setup)
    } finally {
      setup.close()
    }

    const writer = createWorkerDatabaseWriter({ filename, workerID: 41 })
    try {
      const aggregateID = "session-blob-dir"
      const published = await writer.publish({
        id: Event.ID.create(),
        type: SessionEvent.AgentSwitched.type,
        aggregateID,
        data: {
          timestamp: DateTime.makeUnsafe(Date.now()),
          sessionID: aggregateID,
          messageID: "msg_blob_dir",
          agent: `oversized:${"x".repeat(17 * 1024 * 1024)}`,
        },
      })
      expect(published.committed).toBe(true)

      const ownBlobDir = path.join(directory, "event-blobs")
      expect(entries(ownBlobDir)).toHaveLength(1)
      // The global database's sibling directory must never receive this file.
      expect(entries(globalBlobDir)).toEqual(globalBefore)

      // The production EventV2 deletion path over the same custom file collects
      // the blob this worker published, again without touching the global dir.
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const events = yield* EventV2.Service
            yield* events.remove(aggregateID)
          }).pipe(
            Effect.provide(
              EventV2.layerWith().pipe(
                Layer.provide(DurableLog.layerFromDatabase),
                Layer.provide(DatabaseService.layerFromPath(filename)),
              ),
            ),
          ),
        ),
      )
      expect(entries(ownBlobDir)).toEqual([])
      expect(entries(globalBlobDir)).toEqual(globalBefore)
    } finally {
      writer.dispose()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  })
})

// ---------------------------------------------------------------------------
// Worker→hub PID reporting.
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

describe("worker→hub PID reporting", () => {
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
    const recorder = recordingLogger()
    const brokenPort = {
      postMessage: () => {
        throw new Error("port closed")
      },
    } as unknown as MessagePort

    const reporter = createControlPidReporter(brokenPort, 5, { bridge: recorder.bridge })
    expect(() => reporter.report({ pid: 101, pgid: 101, label: "shell:echo hi" })).toThrow("port closed")
    expect(recorder.lines).toEqual([
      {
        message: workerLogMessage("pid report failed"),
        fields: { workerID: 5, reason: "port closed" },
        level: "warn",
      },
    ])
  })

  test("bootstrap forwards its log into the control pid reporter so the failure line fires on a closed port", () => {
    // `bootstrapSessionWorker` builds the reporter over an open MessageChannel and
    // does not expose it, so the forwarding cannot be observed from outside. The
    // `pidReporterFactory` seam hands the test the reporter bootstrap built — and
    // the exact `log` it forwarded — so a throwing port can exercise the failure path without a
    // real closed port (Bun silently accepts a post to a peer-closed port).
    const scope = {
      postMessage(_data: unknown, _transfer?: readonly Transferable[]) {},
      onmessage: null as ((event: { data: unknown }) => void) | null,
    }
    const init: SessionWorkerInit = {
      snapshot: workerSnapshot(),
      env: { values: {} },
      role: "session",
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
    const recorder = recordingLogger()
    const brokenPort = {
      postMessage: () => {
        throw new Error("port closed")
      },
    } as unknown as MessagePort

    let reporter: PidReporterInterface | undefined
    let reportedID: number | undefined
    bootstrapSessionWorker(init, scope, {
      bridge: recorder.bridge,
      pidReporterFactory: (_port, workerID, forwarded) => {
        reportedID = workerID
        reporter = createControlPidReporter(brokenPort, workerID, { bridge: forwarded })
        return reporter
      },
    })

    // The reporter exists and carries the forwarded log: reporting on the broken
    // port logs the WARN through it before rethrowing. The same `log` also carries
    // the client connect lines, so assert on the failure line specifically.
    expect(reporter).toBeDefined()
    expect(() => reporter!.report({ pid: 101, pgid: 101, label: "shell:echo hi" })).toThrow("port closed")
    const failed = recorder.lines.filter((entry) => entry.message === workerLogMessage("pid report failed"))
    expect(failed).toHaveLength(1)
    expect(failed[0].level).toBe("warn")
    expect(failed[0].fields).toEqual({ workerID: reportedID, reason: "port closed" })
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
    REAL_WORKER_SLOW_TEST_TIMEOUT_MS,
  )
})

// ---------------------------------------------------------------------------
// Fix 2 — worker-local ripgrep: the binary path comes from the snapshot and the
// real `AppProcess` spawns report their detached PGID so the hub reaps a
// mid-flight `rg` on cancel/terminate (worker terminate runs NO finalizers).
//
// `Ripgrep.Service` spawns through `AppProcess.spawn`; `RipgrepBinary` is
// snapshot-backed (never locates/downloads). The worker replaces `AppProcess`
// with a reporting wrapper built over a *private* `ChildProcessSpawner`, so the
// shell tool's own `ChildProcessSpawner` reports stay untouched (no double report
// with a mismatched label, which `PidRegistry.register` rejects).
// ---------------------------------------------------------------------------

describe("worker-local ripgrep binary from the snapshot (Fix 2)", () => {
  test("RipgrepBinary.Filepath resolves to the snapshot's ripgrepPath", async () => {
    // `RipgrepBinary.node` is a transitive dependency (via `ToolRegistry`), so
    // `Layer.provide` would hide its service. Promote it to a root of the same
    // runnable graph so the boot wiring's snapshot-backed replacement is directly
    // observable — the rest of the graph (and the client-key assertion) is intact.
    const built = buildSessionWorkerLayer({
      nodes: LayerNode.group([...RUNNABLE_SESSION_NODES.dependencies, RipgrepBinary.node]),
      snapshot: workerSnapshot({ ripgrepPath: "/custom/rg" }),
      env: { values: {} },
      clients: stubClients(),
      modelClient: modelLayer("worker text"),
    })

    const filepath = await Effect.runPromise(
      Effect.gen(function* () {
        const binary = yield* RipgrepBinary.Service
        return yield* binary.filepath
      }).pipe(Effect.provide(built.layer)),
    )

    // The worker never locates or downloads `rg`; it runs exactly the hub's path.
    expect(filepath).toBe("/custom/rg")
  })
})

describe("worker-local AppProcess PID reporting (Fix 2)", () => {
  test("a spawn through the reporting AppProcess reports its detached {pid,pgid}", async () => {
    const reports: Array<{ pid: number; pgid: number; label: string }> = []
    // The fixed-PID fake replaces the underlying `ChildProcessSpawner`, exactly
    // as the shell-tool harness fakes `CrossSpawnSpawner.node` for deterministic
    // PIDs. The reporting wrapper wraps the real `AppProcess` over that fake.
    const layer = workerAppProcessLayer({ report: (entry) => void reports.push(entry) }).pipe(
      Layer.provide(fixedPidSpawnerLayer(4321)),
    )

    await Effect.runPromise(
      Effect.gen(function* () {
        const process = yield* AppProcess.Service
        const handle = yield* Effect.scoped(process.spawn(ChildProcess.make("rg", ["--version"])))
        expect(Number(handle.pid)).toBe(4321)
      }).pipe(Effect.provide(layer)),
    )

    // POSIX detached children lead their own group, so `pgid === pid`.
    expect(reports).toEqual([{ pid: 4321, pgid: 4321, label: "rg --version" }])
  })

  test("delegates the spawn unchanged and stays silent without a reporter", async () => {
    const layer = workerAppProcessLayer(undefined).pipe(Layer.provide(fixedPidSpawnerLayer(7)))

    const pid = await Effect.runPromise(
      Effect.gen(function* () {
        const process = yield* AppProcess.Service
        const handle = yield* Effect.scoped(process.spawn(ChildProcess.make("rg", ["--files"])))
        return Number(handle.pid)
      }).pipe(Effect.provide(layer)),
    )

    // A degraded/absent reporter must not change the spawn's observable result.
    expect(pid).toBe(7)
  })
})

// ---------------------------------------------------------------------------
// Work-kind registry.
// ---------------------------------------------------------------------------

describe("createWorkRegistry", () => {
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

  test("the production registry runs session work only for the protocol literal", () => {
    const registry = productionWorkRegistry(marker)
    expect(registry("session")).toBe(marker)
    expect(registry(DEFAULT_WORK_KIND)).toBe(marker)
    expect(() => registry("other")).toThrow(UnknownWorkKindError)
  })
})

// ---------------------------------------------------------------------------
// Real session work.
// ---------------------------------------------------------------------------

describe("createSessionWork", () => {
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
// The carried sub-agent prompt. A dispatched child's prompt crosses the
// executor frame; the worker must persist that user message and loop rather
// than call a bare `loop` that expects the row to already exist. A prompt-less
// run is unchanged.
// ---------------------------------------------------------------------------

describe("carried prompt (createSessionWork)", () => {
  /** A `SessionPrompt` service that records which entry point was taken. */
  const promptServiceLayer = (record: string[]): Layer.Layer<unknown, unknown> =>
    Layer.succeed(SessionPrompt.Service, {
      cancel: () => Effect.succeed(undefined),
      prompt: (input) => {
        record.push("prompt")
        return Effect.succeed(withParts(input.sessionID))
      },
      loop: (input) => {
        record.push("loop")
        return Effect.succeed(withParts(input.sessionID))
      },
      shell: () => Effect.die("the shell path is not exercised here"),
      command: () => Effect.die("the command path is not exercised here"),
      resolvePromptParts: () => Effect.succeed([]),
    }) as unknown as Layer.Layer<unknown, unknown>

  const writerFor = (queries: string[]) => ({
    query: async (_method: string, sql: string) => {
      queries.push(sql)
      return []
    },
    publish: async () => ({ seq: 0, committed: false }),
  })

  test("a carried prompt persists the user message and loops via SessionPrompt.prompt", async () => {
    const record: string[] = []
    const queries: string[] = []
    const work = createSessionWork({
      layer: promptServiceLayer(record),
      writer: writerFor(queries),
      threadId: 0,
      prompt: true,
    })
    const input: PromptInput = { sessionID: sid("ses_p"), parts: [{ type: "text", text: "hi" }] }

    const result = await Effect.runPromise(work(sid("ses_p"), input))

    expect(record).toEqual(["prompt"])
    expect(result.sessionID).toBe(sid("ses_p"))
    expect(result.value).toEqual(withParts(sid("ses_p")))
  })

  test("a prompt-less run loops as today and writes nothing", async () => {
    const record: string[] = []
    const queries: string[] = []
    const work = createSessionWork({
      layer: promptServiceLayer(record),
      writer: writerFor(queries),
      threadId: 0,
      prompt: true,
    })

    await Effect.runPromise(work(sid("ses_q")))

    expect(record).toEqual(["loop"])
    expect(queries).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// The carried shell. A dispatched shell's input crosses the executor frame;
// the worker must replay the caller's full `SessionPrompt.shell` path (persist +
// shell + settle) rather than the bare `loop`. The run's `sessionID` is
// authoritative, so a stale id on the carried shell never wins. The executor
// passes the shell as the third `SessionWork` argument, so the frozen contract
// is `createSessionWork(sessionID, carriedPrompt, carriedShell)`.
// ---------------------------------------------------------------------------

describe("carried shell (createSessionWork)", () => {
  /** A `SessionPrompt` service that records the shell input; loop/prompt die. */
  const shellServiceLayer = (record: Array<Record<string, unknown>>): Layer.Layer<unknown, unknown> =>
    Layer.succeed(SessionPrompt.Service, {
      cancel: () => Effect.succeed(undefined),
      prompt: () => Effect.die("the prompt path is not exercised here"),
      loop: () => Effect.die("the loop path is not exercised here"),
      shell: (input) => {
        record.push(input as unknown as Record<string, unknown>)
        return Effect.succeed(withParts(input.sessionID))
      },
      command: () => Effect.die("the command path is not exercised here"),
      resolvePromptParts: () => Effect.succeed([]),
    }) as unknown as Layer.Layer<unknown, unknown>

  const writerFor = () => ({
    query: async () => [],
    publish: async () => ({ seq: 0, committed: false }),
  })

  test("a carried shell runs SessionPrompt.shell with the run's sessionID, not loop", async () => {
    const shells: Array<Record<string, unknown>> = []
    const work = createSessionWork({
      layer: shellServiceLayer(shells),
      writer: writerFor(),
      threadId: 0,
      prompt: true,
    })
    // A stale sessionID on the carried shell: the run's id must win because the
    // implementation spreads `{ ...carriedShell, sessionID }`.
    const carried = { sessionID: sid("ses_stale"), agent: "build", command: "echo hi" }

    const result = await Effect.runPromise(work(sid("ses_shell"), undefined, carried))

    expect(shells).toHaveLength(1)
    expect(shells[0]).toEqual({ sessionID: sid("ses_shell"), agent: "build", command: "echo hi" })
    expect(result.sessionID).toBe(sid("ses_shell"))
    expect(result.value).toEqual(withParts(sid("ses_shell")))
  })

  test("a carried shell takes precedence over a carried prompt", async () => {
    const shells: Array<Record<string, unknown>> = []
    const work = createSessionWork({
      layer: shellServiceLayer(shells),
      writer: writerFor(),
      threadId: 0,
      prompt: true,
    })
    const input: PromptInput = { sessionID: sid("ses_p"), parts: [{ type: "text", text: "hi" }] }
    const carried = { sessionID: sid("ses_stale"), agent: "build", command: "pwd" }

    await Effect.runPromise(work(sid("ses_both"), input, carried))

    // Only the shell path ran; the prompt path died if it were taken.
    expect(shells).toHaveLength(1)
    expect(shells[0]).toEqual({ sessionID: sid("ses_both"), agent: "build", command: "pwd" })
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
    const bootstrap = bootstrapSessionWorker(
      {
        snapshot: workerSnapshot(),
        env: { values: {} },
        role: "session",
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
    try {
      const ports = frames.find((frame) => (frame.data as { type?: string }).type === "ports")
      expect(ports).toBeDefined()
      expect((ports!.data as { egress?: unknown }).egress).toBeInstanceOf(MessagePort)
    } finally {
      // This is a real bootstrap (graph, DB, event bridge). Dispose it and close
      // every transferred port so the worker never outlives the test and poisons
      // a later E2E run that starts its own model loop.
      for (const frame of frames) {
        for (const value of Object.values(frame.data as Record<string, unknown>)) {
          if (value instanceof MessagePort) value.close()
        }
      }
      bootstrap.dispose()
    }
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
        "sessionStatus.get": (sessionID) => store.get(sessionID as string) ?? { type: "idle" },
        "sessionStatus.set": (sessionID, status) => {
          store.set(sessionID as string, status as SessionStatus.Info)
        },
        "sessionStatus.list": () => [...store.entries()],
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

      // 4. Every durable event is written through the worker's own WAL
      //    connection (D5), and event ids stay unique.
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
// to the hub-side sink. `session.error` was previously only unit-covered at the
// hub sink, never over a real worker error run.
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
// A git-backed project must not crash the worker.
//
// The worker has no OS child-process authority (`AppProcess` is a hub-owned
// node), so the real `Snapshot` service dies with `UnimplementedError` the
// moment it shells out to `git`. The worker installs a fail-soft `Snapshot`
// boundary instead; the session still completes, only snapshot/revert is
// unavailable in the isolate.
// ---------------------------------------------------------------------------

describe("worker Snapshot boundary for a git-backed project", () => {
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
// The worker receives the REAL model descriptor, not a bare identity.
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
      role: "session",
      model: descriptorModel,
    })

    expect(parsed.model).toEqual(descriptorModel)
    expect(parsed.model.limit.output).toBe(32_000)
    expect(parsed.model.capabilities.temperature).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// Cancelling a worker-local session must STOP the real
// `SessionPrompt.loop` (the worker-local `SessionRunState` runner, table B), not
// just the executor's outer machine (table A). The old cancel path interrupted
// the outer fiber while table B's `runLoop` survived; the run control seam routes
// cancel/assertNotBusy to table B.
// ---------------------------------------------------------------------------

/** Poll a predicate until it is true, failing loud on timeout. */
async function waitUntil(
  predicate: () => boolean | Promise<boolean>,
  message: string,
  // A cold worker import graph plus the provider round-trip can exceed a tight
  // budget on a loaded host; the deadline guards against a hang, not latency.
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
  throw new Error(`timed out: ${message}`)
}

describe("real SessionPrompt.loop cancellation", () => {
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

      // 6.3: the run-state reflects the REAL loop after cancel, not a stale
      // table A. The per-run graph scope may already be disposed by the time
      // cancel settles; an interrupted probe then means no runner can be busy,
      // so it is the same idle answer.
      const busyAfterCancel = await control!.busy(sessionID).catch(() => false)
      expect(busyAfterCancel).toBe(false)

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
