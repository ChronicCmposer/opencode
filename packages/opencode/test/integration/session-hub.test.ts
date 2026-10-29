// Bound-worker session boundary — a worker stays single-session, yet subagents work.
//
// This drives a REAL bound worker isolate (production worker database opened with
// `sessionID`, production reduced graph, real `MessageChannel` control port) and
// a real hub side (real `Session` over SQLite, the production handler map, and
// the production spawn authority). It asserts the four operations the boundary fix
// routes through the hub — create, resume/read, children, cancel cascade — and
// that every successful worker SQL statement stays inside the parent session.
//
// The child row is created by the HUB, read by the HUB, listed by the HUB, and
// cancelled through the HUB. A direct worker read/write of the child row is
// refused by the worker DB guard, and the recorded successful SQL never names
// the child or grandchild.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test"
import { Context, Effect, Exit, Layer, Option, Scope } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import type { Permission } from "@/permission"
import type { Question } from "@/question"
import type { SessionStatus } from "@/session/status"
import { cancelSessionTree, type SessionPrompt } from "@/session/prompt"
import { SessionPromptService } from "@/session/prompt-service"
import { MessageID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { markSchemaIdentity } from "@/session/db/schema"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  boundedDispatchDepth,
  buildHubProxyHandlers,
  createSessionChildrenIndex,
  hubProxyAuthoritiesFor,
  sessionSpawnFor,
  spawnChildOnHub,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { createLazyExecutorWorkerPool } from "../lib/lazy-worker-pool"
import type { ExecutorWorker, ExecutorWorkerPool } from "@/session/worker/execute"
import { serveHubProxy } from "@/session/worker/hub-proxy-wire"
import type { CancelOutcome } from "@/session/worker/cancel"
import { recordingHubAuthorities } from "../lib/hub-proxy"
import { withDeadline } from "../lib/async"
import { REAL_WORKER_TEST_TIMEOUT_MS } from "../lib/worker-timeouts"

// Bun resets the per-file default to 5s; this file boots a real worker isolate.
setDefaultTimeout(REAL_WORKER_TEST_TIMEOUT_MS)

const WORKER = new URL("../fixture/session-hub-worker.ts", import.meta.url)
const ROOT = SessionID.make("ses_session_hub_root")
const PARENT = SessionID.make("ses_session_hub_parent")
const PROJECT = "prj_session_hub"
const DIRECTORIES: string[] = []
const CLEANUPS: Array<() => void | Promise<void>> = []

/** The prompt authority's shared service key, exactly as the hub resolver reads it. */
function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-session-hub-"))
  DIRECTORIES.push(directory)
  return directory
}

function migratedDatabase(filename: string): void {
  const connection = openConnection({ filename })
  try {
    for (const statement of migrationStatements) connection.exec(statement)
    markSchemaIdentity(connection)
  } finally {
    connection.close()
  }
}

function seedParent(filename: string, directory: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "session-hub", directory, "[]", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [ROOT, PROJECT, "root", directory, "root", "1", 1, 1],
    )
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [PARENT, PROJECT, "parent", directory, "parent", "1", 1, 1, ROOT],
    )
  } finally {
    connection.close()
  }
}

function row(filename: string, sessionID: string) {
  const connection = openConnection({ filename })
  try {
    return connection.all<{
      readonly id: string
      readonly parent_id: string | null
      readonly title: string
      readonly metadata: string | null
    }>("SELECT id, parent_id, title, metadata FROM session WHERE id = ?", [sessionID])[0]
  } finally {
    connection.close()
  }
}

function childReply(input: SessionPrompt.PromptInput): SessionV1.WithParts {
  const id = MessageID.ascending()
  return {
    info: {
      id,
      role: "assistant",
      parentID: MessageID.ascending(),
      sessionID: input.sessionID,
      mode: input.agent ?? "general",
      agent: input.agent ?? "general",
      cost: 0,
      path: { cwd: "/tmp", root: "/tmp" },
      tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      modelID: "mock-model" as never,
      providerID: "mock" as never,
      time: { created: Date.now() },
      finish: "stop",
    },
    parts: [
      {
        id: "part-session-hub-reply" as never,
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text: "child replied",
      },
    ],
  }
}

function cancelOutcome(sessionID: SessionID): CancelOutcome {
  return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
}

/** A real children-first cancel over the test's sessions, recording the order. */
function recordingPrompt(
  sessions: Session.Interface,
  prompts: SessionPrompt.PromptInput[],
  cancelOrder: SessionID[],
): SessionPromptService.Interface {
  return {
    prompt: (input: SessionPrompt.PromptInput) =>
      Effect.sync(() => {
        prompts.push(input)
        return childReply(input)
      }),
    cancel: (sessionID: SessionID) =>
      cancelSessionTree({
        children: (id) => sessions.children(id),
        cancelOne: (id) =>
          Effect.sync(() => {
            cancelOrder.push(id)
            return cancelOutcome(id)
          }),
      })(sessionID),
  } as unknown as SessionPromptService.Interface
}

async function openHub(
  filename: string,
  directory: string,
  makePrompt: (sessions: Session.Interface) => SessionPromptService.Interface,
): Promise<{ readonly sessions: Session.Interface; readonly run: HubProxyRunner }> {
  const hubLayer = LayerNode.compile(
    LayerNode.group([
      Session.node,
      SessionProjector.node,
      EventV2Bridge.node,
      BackgroundJob.node,
      RuntimeFlags.node,
      Database.node,
    ]),
    [
      [Database.node, Database.layerFromPath(filename)],
      [RuntimeFlags.node, RuntimeFlags.layer({ experimentalSessionWorkers: false })],
    ],
  )
  const hubScope = Effect.runSync(Scope.make())
  CLEANUPS.push(async () => {
    await Effect.runPromise(Scope.close(hubScope, Exit.void))
  })
  const hubContext = await Effect.runPromise(Layer.buildWithScope(hubLayer, hubScope))
  const sessions = Context.get(hubContext, Session.Service)
  const prompt = makePrompt(sessions)
  const instance = {
    directory,
    worktree: directory,
    project: { id: PROJECT, worktree: directory, vcs: "git" },
  } as never
  const run: HubProxyRunner = (effect) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provide(hubContext),
        Effect.provideService(InstanceRef, instance),
        Effect.provideService(SessionPromptService.Service, prompt),
      ),
    )
  return { sessions, run }
}

interface WorkerResult {
  readonly id: number
  readonly ok: boolean
  readonly value?: unknown
  readonly error?: string
  readonly tag?: string
  readonly sql: readonly string[]
}

interface WorkerHarness {
  call(op: string, arg?: unknown): Promise<WorkerResult>
  sql(): readonly string[]
  close(): Promise<void>
}

function startWorker(filename: string, directory: string, port: MessagePort): Promise<WorkerHarness> {
  const worker = new Worker(WORKER, {
    workerData: { filename, sessionID: String(PARENT), directory },
  } as unknown as WorkerOptions)
  const messages: unknown[] = []
  const waiters = new Set<() => void>()
  const pending = new Map<number, { resolve: (value: WorkerResult) => void; reject: (error: Error) => void }>()
  let nextID = 1
  let lastSql: readonly string[] = []

  const notify = () => {
    for (const waiter of [...waiters]) waiter()
  }
  worker.addEventListener("message", (event: MessageEvent) => {
    const message = event.data as { readonly type?: string }
    if (message?.type === "result") {
      const result = message as WorkerResult
      lastSql = result.sql
      pending.get(result.id)?.resolve(result)
      pending.delete(result.id)
      return
    }
    messages.push(message)
    notify()
    if (message?.type === "error") {
      for (const entry of pending.values()) entry.reject(new Error(String((message as { error?: unknown }).error)))
      pending.clear()
    }
  })
  worker.addEventListener("error", (event: ErrorEvent) => {
    const error = event.error instanceof Error ? event.error : new Error(String(event.message))
    for (const entry of pending.values()) entry.reject(error)
    pending.clear()
  })

  const waitFor = (type: string, timeoutMs = 15_000): Promise<void> =>
    new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        waiters.delete(check)
        reject(new Error(`worker never sent "${type}"`))
      }, timeoutMs)
      const check = () => {
        const index = messages.findIndex((message) => (message as { type?: string }).type === type)
        if (index < 0) return
        messages.splice(index, 1)
        clearTimeout(timer)
        waiters.delete(check)
        resolve()
      }
      waiters.add(check)
      check()
    })

  const initialized = (async () => {
    await waitFor("ready")
    worker.postMessage({ type: "hub-port", port }, [port])
    await waitFor("initialized")
  })()

  return initialized.then(() => ({
    call: (op, arg) =>
      new Promise<WorkerResult>((resolve, reject) => {
        const id = nextID++
        pending.set(id, { resolve, reject })
        worker.postMessage({ type: "call", id, op, arg })
      }),
    sql: () => lastSql,
    close: async () => {
      worker.postMessage({ type: "close" })
      await worker.terminate()
    },
  }))
}

afterEach(async () => {
  for (const cleanup of CLEANUPS.splice(0).reverse()) await cleanup()
  for (const directory of DIRECTORIES.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

/** A one-shot signal: `promise` resolves on the first `resolve()` call. */
function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>((settle) => {
    resolve = settle
  })
  return { promise, resolve }
}

/** A scripted `ExecutorWorker`, wrapped by the cap exactly as production does. */
function scriptedWorker(
  id: number,
  execute: (run: { readonly sessionID: SessionID; readonly depth: number }) => Promise<void>,
): ExecutorWorker {
  return {
    id,
    alive: true,
    execute: execute as ExecutorWorker["execute"],
    cancel: async () => {},
    assertNotBusy: async () => false,
    resultFor: () => undefined,
    terminate: async () => {},
  }
}

describe("session-hub boundary", () => {
  test("create, resume, children and cancel all cross the hub; the worker never touches the child row", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedParent(filename, directory)

    const prompts: SessionPrompt.PromptInput[] = []
    const cancelOrder: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, (service) =>
      recordingPrompt(service, prompts, cancelOrder),
    )
    const depths = new Map<SessionID, number>()
    const childEdges = createSessionChildrenIndex()
    const base = recordingHubAuthorities()
    const authorities = hubProxyAuthoritiesFor({
      run,
      boundSessionID: PARENT,
      subagentDepth: 10,
      recordDepth: (sessionID, depth) => {
        if (depth > 0) depths.set(sessionID, depth)
        else depths.delete(sessionID)
      },
      registerChild: childEdges.register,
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })

    const channel = new MessageChannel()
    const disposeServer = serveHubProxy(buildHubProxyHandlers(authorities, run), channel.port2)
    CLEANUPS.push(() => {
      disposeServer()
      try {
        channel.port1.close()
      } catch {
        // The port was transferred to the worker; the worker owns it now.
      }
      channel.port2.close()
    })

    const worker = await startWorker(filename, directory, channel.port1)
    CLEANUPS.push(() => worker.close())

    // 1. Create: the child row is written by the hub, not the worker.
    const created = await worker.call("create", { title: "hub child" })
    expect(created.ok).toBe(true)
    const childID = (created.value as Session.Info).id
    expect(row(filename, String(childID))?.parent_id).toBe(String(PARENT))

    // 2. Read (the task-tool resume path): the worker gets the child Info from
    // the hub and never from its own database.
    const read = await worker.call("get", { id: childID })
    expect(read.ok).toBe(true)
    expect((read.value as Session.Info).id).toBe(childID)

    // 3. Resume: Session.spawn stamps the child's depth and prompts on the hub.
    const first = await worker.call("spawn", { childID, text: "first turn" })
    expect(first.ok, first.error).toBe(true)
    expect((first.value as { childID: string }).childID).toBe(childID)
    const second = await worker.call("spawn", { childID, text: "resumed turn" })
    expect(second.ok, second.error).toBe(true)
    expect(prompts.map((input) => String(input.sessionID))).toEqual([String(childID), String(childID)])
    expect(JSON.parse(row(filename, String(childID))?.metadata ?? "{}")).toMatchObject({ depth: 1 })
    // The depth mirror the dispatcher's cap reads comes from the persisted
    // parent, and the crash-cascade edge is registered before the child runs.
    expect(depths.get(childID)).toBe(1)
    expect(childEdges.childrenOf(PARENT)).toEqual([childID])

    // 4. Children: the parent worker lists its child through the hub.
    const children = await worker.call("children")
    expect(children.ok).toBe(true)
    expect((children.value as Session.Info[]).map((info) => info.id)).toEqual([childID])

    // 5. Cancel cascade: a grandchild is created on the hub, then the bound
    // worker's SessionPrompt.cancel crosses the hub and cancels children-first.
    const grandchild = await run(sessions.create({ parentID: childID, title: "grandchild" }))
    const cancelled = await worker.call("cancel", { id: PARENT })
    expect(cancelled.ok, cancelled.error).toBe(true)
    expect(cancelOrder).toEqual([grandchild.id, childID, PARENT])

    // 6. The worker never touches the child row directly.
    const directRead = await worker.call("directRead", { id: childID })
    expect(directRead.ok).toBe(true)
    const readProbe = directRead.value as { kind: "read" | "rejected"; rows?: readonly unknown[]; tag?: string }
    // The worker's bound view resolves the child id to zero rows; it never
    // observes the child row itself.
    if (readProbe.kind === "read") expect(readProbe.rows).toEqual([])
    else expect(readProbe.tag).toContain("CrossSessionAccessError")
    // The repository boundary may reject the statement plain or scope it to zero
    // rows; either way the child row is unchanged, which is the security
    // property under test.
    await worker.call("directWrite", { id: childID })
    expect(row(filename, String(childID))?.title).toBe("hub child")

    const successfulSql = worker.sql()
    expect(successfulSql.some((statement) => statement.includes(String(childID)))).toBe(false)
    expect(successfulSql.some((statement) => statement.includes(String(grandchild.id)))).toBe(false)
    expect(row(filename, String(childID))?.id).toBe(String(childID))
  }, 60_000)

  test("spawn rejects foreign, sibling, ancestor and descendant targets", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedParent(filename, directory)

    const prompts: SessionPrompt.PromptInput[] = []
    const cancelOrder: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, (service) =>
      recordingPrompt(service, prompts, cancelOrder),
    )
    const depths = new Map<SessionID, number>()
    const edges = createSessionChildrenIndex()
    const base = recordingHubAuthorities()
    const deps = {
      run,
      subagentDepth: 10,
      recordDepth: (sessionID: SessionID, depth: number) => {
        if (depth > 0) depths.set(sessionID, depth)
        else depths.delete(sessionID)
      },
      registerChild: edges.register,
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    }
    const authorities = hubProxyAuthoritiesFor({ ...deps, boundSessionID: PARENT })
    const spawn = sessionSpawnFor(PARENT, deps)

    const prompt = (sessionID: SessionID): SessionPrompt.PromptInput => ({
      messageID: MessageID.ascending(),
      sessionID,
      parts: [{ type: "text", text: "spawn probe" }],
    })
    const spawnRequest = (parentID: SessionID, childID?: SessionID) => ({
      parentID,
      ...(childID === undefined ? {} : { childID }),
      type: "task",
      prompt: prompt(childID ?? parentID),
    })

    // A direct child and its own child; a sibling under the shared root; and a
    // wholly foreign child under another root.
    const child = await run(sessions.create({ parentID: PARENT, title: "direct child" }))
    const grandchild = await run(sessions.create({ parentID: child.id, title: "grandchild" }))
    const sibling = await run(sessions.create({ parentID: ROOT, title: "sibling" }))
    const foreignRoot = await run(sessions.create({ title: "foreign root" }))
    const foreignChild = await run(sessions.create({ parentID: foreignRoot.id, title: "foreign child" }))

    // Parent-side rejections: sibling and ancestor parents.
    await expect(spawn(spawnRequest(sibling.id))).rejects.toThrow(/may not spawn under/)
    await expect(spawn(spawnRequest(ROOT))).rejects.toThrow(/may not spawn under/)

    // Child-side rejections: foreign, descendant (grandchild) and self.
    await expect(spawn(spawnRequest(PARENT, foreignChild.id))).rejects.toThrow(/not a direct child/)
    await expect(spawn(spawnRequest(PARENT, grandchild.id))).rejects.toThrow(/not a direct child/)
    await expect(spawn(spawnRequest(PARENT, PARENT))).rejects.toThrow(/may not resume its own session/)
    expect(prompts).toHaveLength(0)
    expect(depths.size).toBe(0)
    expect(edges.childrenOf(PARENT)).toEqual([])

    // A direct child resume is the one allowed shape.
    const resumed = await spawn(spawnRequest(PARENT, child.id))
    expect(resumed.childID).toBe(child.id)
    expect(prompts.map((input) => input.sessionID)).toEqual([child.id])
    expect(depths.get(child.id)).toBe(1)
    expect(edges.childrenOf(PARENT)).toEqual([child.id])

    // Create, read, list and cancel are bound to the same subtree.
    await expect(
      authorities.sessionCreate!({ sessionID: PARENT, parentID: foreignRoot.id, title: "not mine" }),
    ).rejects.toThrow(/may not create a child under/)
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: sibling.id })).rejects.toThrow(
      /may not read session/,
    )
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: ROOT })).rejects.toThrow(/may not read session/)
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: foreignChild.id })).rejects.toThrow(
      /may not read session/,
    )
    await expect(authorities.sessionChildren!({ sessionID: PARENT, parentID: sibling.id })).rejects.toThrow(
      /may not list children/,
    )
    await expect(authorities.sessionCancel!({ sessionID: PARENT, targetID: sibling.id })).rejects.toThrow(
      /may not cancel session/,
    )
    await expect(authorities.sessionCancel!({ sessionID: PARENT, targetID: ROOT })).rejects.toThrow(
      /may not cancel session/,
    )

    // Self and grandchild ids are outside a parent-bound child scope; a direct
    // child stays readable and cancellable through the hub.
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: PARENT })).rejects.toThrow(
      /may not read session/,
    )
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: grandchild.id })).rejects.toThrow(
      /may not read session/,
    )
    await expect(authorities.sessionChildren!({ sessionID: PARENT, parentID: child.id })).rejects.toThrow(
      /may not list children/,
    )
    await expect(authorities.sessionCancel!({ sessionID: PARENT, targetID: grandchild.id })).rejects.toThrow(
      /may not cancel session/,
    )
    const ownCancel = await authorities.sessionCancel!({ sessionID: PARENT, targetID: PARENT })
    expect(ownCancel?.sessionID).toBe(PARENT)
    const childCancel = await authorities.sessionCancel!({ sessionID: PARENT, targetID: child.id })
    expect(childCancel?.sessionID).toBe(child.id)

    // A hub create never trusts caller depth: it stamps the persisted parent's
    // authoritative depth, which is what the dispatcher's cap headroom reads.
    const forged = await authorities.sessionCreate!({
      sessionID: PARENT,
      parentID: PARENT,
      title: "forged depth",
      metadata: { depth: 99 },
    })
    expect(JSON.parse(row(filename, String(forged.id))?.metadata ?? "{}")).toMatchObject({ depth: 1 })

    // An unregistered worker cannot claim a binding through the request.
    const unbound = hubProxyAuthoritiesFor({ ...deps, boundSessionID: undefined })
    await expect(unbound.sessionSpawn!(spawnRequest(PARENT))).rejects.toThrow(/no bound session/)
  }, 60_000)

  test("spawn ignores request.metadata.depth and enforces the parent's depth cap", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedParent(filename, directory)

    const prompts: SessionPrompt.PromptInput[] = []
    const cancelOrder: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, (service) =>
      recordingPrompt(service, prompts, cancelOrder),
    )
    const depths = new Map<SessionID, number>()
    const base = recordingHubAuthorities()
    // The parent is already at depth 1, so the next child (depth 2) exceeds the
    // hub's resolved cap of 1. A worker-supplied depth of 0 must not bypass it.
    const deepParent = await run(sessions.create({ title: "deep parent", metadata: { depth: 1 } }))
    const authorities = hubProxyAuthoritiesFor({
      run,
      boundSessionID: deepParent.id,
      subagentDepth: 1,
      recordDepth: (sessionID, depth) => {
        if (depth > 0) depths.set(sessionID, depth)
        else depths.delete(sessionID)
      },
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })
    const request = {
      parentID: deepParent.id,
      type: "task",
      metadata: { depth: 0 },
      prompt: {
        messageID: MessageID.ascending(),
        sessionID: deepParent.id,
        parts: [{ type: "text" as const, text: "over the cap" }],
      },
    }

    await expect(authorities.sessionSpawn!(request)).rejects.toThrow(/Subagent depth limit reached \(1\)/)
    expect(await run(sessions.children(deepParent.id))).toEqual([])
    expect(prompts).toHaveLength(0)
    expect(depths.size).toBe(0)
  }, 60_000)

  test("a recorded depth-1 task starts while its parent owns maxRunning=1", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedParent(filename, directory)

    const prompts: SessionPrompt.PromptInput[] = []
    const cancelOrder: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, (service) =>
      recordingPrompt(service, prompts, cancelOrder),
    )
    const depths = new Map<SessionID, number>()
    const base = recordingHubAuthorities()
    const authorities = hubProxyAuthoritiesFor({
      run,
      boundSessionID: PARENT,
      subagentDepth: 10,
      recordDepth: (sessionID, depth) => {
        if (depth > 0) depths.set(sessionID, depth)
        else depths.delete(sessionID)
      },
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })
    const child = await run(sessions.create({ parentID: PARENT, title: "foreground task" }))
    // The request lies about depth 0; the persisted parent still records 1.
    const spawned = await authorities.sessionSpawn!({
      parentID: PARENT,
      childID: child.id,
      type: "task",
      metadata: { depth: 0 },
      prompt: {
        messageID: MessageID.ascending(),
        sessionID: child.id,
        parts: [{ type: "text", text: "run me" }],
      },
    })
    expect(spawned.childID).toBe(child.id)
    expect(depths.get(child.id)).toBe(1)
    // The forged request depth 0 is not persisted: the hub stamps 1 from the
    // persisted parent before the child can dispatch.
    expect(JSON.parse(row(filename, String(child.id))?.metadata ?? "{}")).toMatchObject({ depth: 1 })

    const parentStarted = deferred()
    const parentRelease = deferred()
    const childStarted = deferred()
    const childRelease = deferred()
    let pool: ExecutorWorkerPool
    pool = createLazyExecutorWorkerPool({
      maxRunning: 1,
      create: (id) =>
        scriptedWorker(id, async (current) => {
          if (current.depth === 0) {
            parentStarted.resolve()
            await parentRelease.promise
            return
          }
          childStarted.resolve()
          await childRelease.promise
        }),
    })
    const parentRun = pool.spawn(1).execute({ sessionID: PARENT, depth: 0 })
    await withDeadline(parentStarted.promise, 2_000, "the parent never took the only foreground slot")
    // This is the production dispatch depth: the recorded map, clamped exactly
    // as `createSessionHub` does before `cap.acquire`.
    const childRun = pool.spawn(2).execute({ sessionID: child.id, depth: boundedDispatchDepth(depths.get(child.id)) })
    await withDeadline(
      childStarted.promise,
      2_000,
      "the task ran at depth 0 and starved behind its parent under maxRunning=1",
    )
    childRelease.resolve()
    parentRelease.resolve()
    await Promise.all([parentRun, childRun])
  }, 30_000)

  test("hub spawn strips a caller-forged metadata.depth before Session.spawn", async () => {
    const captured: Array<{ readonly metadata?: Record<string, unknown> }> = []
    const sessions = {
      spawn: (input: { readonly metadata?: Record<string, unknown> }) => {
        captured.push(input)
        return Effect.succeed({ childID: PARENT, result: {} as never })
      },
    } as unknown as Session.Interface

    await Effect.runPromise(
      spawnChildOnHub(sessions, 4, {
        parentID: PARENT,
        type: "task",
        metadata: { depth: 0, keep: "caller data" },
        prompt: {
          messageID: MessageID.ascending(),
          sessionID: PARENT,
          parts: [{ type: "text", text: "depth probe" }],
        },
      }),
    )
    // Depth is derived from the persisted parent inside Session.spawn; caller
    // metadata survives, but never its depth.
    expect(captured[0]?.metadata).toEqual({ keep: "caller data" })
  })

  test("worker authorities reject or ignore foreign session and request ids", async () => {
    const BOUND = SessionID.make("ses_bound_authority")
    const FOREIGN = SessionID.make("ses_foreign_authority")
    const permissionRequests = [
      { id: "per_bound", sessionID: BOUND, permission: "read", patterns: ["*"], always: [] },
      { id: "per_foreign", sessionID: FOREIGN, permission: "read", patterns: ["*"], always: [] },
    ] as never
    const questionRequests = [
      { id: "que_bound", sessionID: BOUND, questions: [] },
      { id: "que_foreign", sessionID: FOREIGN, questions: [] },
    ] as never
    const permissionAsks: string[] = []
    const permissionReplies: string[] = []
    const questionAsks: string[] = []
    const questionReplies: string[] = []
    const statusGets: string[] = []
    const statusCalls: string[] = []

    const permission: Permission.Interface = {
      ask: (input) =>
        Effect.sync(() => {
          permissionAsks.push(String(input.sessionID))
        }),
      reply: (input) =>
        Effect.sync(() => {
          permissionReplies.push(String(input.requestID))
        }),
      list: () => Effect.succeed(permissionRequests),
    }
    const question: Question.Interface = {
      ask: (input) =>
        Effect.sync(() => {
          questionAsks.push(String(input.sessionID))
          return []
        }),
      reply: (input) =>
        Effect.sync(() => {
          questionReplies.push(String(input.requestID))
        }),
      reject: (requestID) =>
        Effect.sync(() => {
          questionReplies.push(String(requestID))
        }),
      list: () => Effect.succeed(questionRequests),
    }
    const sessionStatus: SessionStatus.Interface = {
      get: (sessionID) =>
        Effect.sync(() => {
          statusGets.push(String(sessionID))
          return { type: "idle" as const }
        }),
      set: (sessionID, status) =>
        Effect.sync(() => {
          statusCalls.push(`${String(sessionID)}:${status.type}`)
        }),
      list: () =>
        Effect.succeed(
          new Map([
            [BOUND, { type: "busy" as const }],
            [FOREIGN, { type: "idle" as const }],
          ]),
        ),
    }
    const authorities = hubProxyAuthoritiesFor({
      run: (effect) => Effect.runPromise(effect),
      boundSessionID: BOUND,
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus,
      permission,
      question,
    })

    // Lists only expose the bound session's requests.
    const listedPermission = (await Effect.runPromise(authorities.permission.list())) as ReadonlyArray<{ id: string }>
    expect(listedPermission.map((item) => item.id)).toEqual(["per_bound"])
    const listedQuestion = (await Effect.runPromise(authorities.question.list())) as ReadonlyArray<{ id: string }>
    expect(listedQuestion.map((item) => item.id)).toEqual(["que_bound"])

    const permissionAsk = (sessionID: SessionID) =>
      ({ sessionID, permission: "read", patterns: ["*"], metadata: {}, always: [], ruleset: [] }) as never
    const questionAsk = (sessionID: SessionID) => ({ sessionID, questions: [] }) as never

    // Asks may only name the bound session; a foreign ask never reaches the
    // hub-owned prompt registry.
    await expect(Effect.runPromise(authorities.permission.ask(permissionAsk(FOREIGN)))).rejects.toThrow()
    expect(permissionAsks).toEqual([])
    await Effect.runPromise(authorities.permission.ask(permissionAsk(BOUND)))
    expect(permissionAsks).toEqual([String(BOUND)])
    await expect(Effect.runPromise(authorities.question.ask(questionAsk(FOREIGN)))).rejects.toThrow()
    expect(questionAsks).toEqual([])
    await Effect.runPromise(authorities.question.ask(questionAsk(BOUND)))
    expect(questionAsks).toEqual([String(BOUND)])

    // Status reads are bound too: a foreign read never reaches the service and
    // resolves to idle, while the list drops every other session's entry.
    expect(await Effect.runPromise(authorities.sessionStatus.get(FOREIGN))).toEqual({ type: "idle" })
    expect(statusGets).toEqual([])
    expect(await Effect.runPromise(authorities.sessionStatus.get(BOUND))).toEqual({ type: "idle" })
    expect(statusGets).toEqual([String(BOUND)])
    const listedStatus = await Effect.runPromise(authorities.sessionStatus.list())
    expect([...listedStatus.keys()]).toEqual([BOUND])
    expect(listedStatus.get(FOREIGN)).toBeUndefined()

    // Foreign status writes are ignored; the bound one lands.
    await Effect.runPromise(authorities.sessionStatus.set(FOREIGN, { type: "busy" } as never))
    expect(statusCalls).toEqual([])
    await Effect.runPromise(authorities.sessionStatus.set(BOUND, { type: "busy" } as never))
    expect(statusCalls).toEqual([`${String(BOUND)}:busy`])

    // Foreign permission/question ids are denied without reaching the service.
    await expect(
      Effect.runPromise(authorities.permission.reply({ requestID: "per_foreign", reply: "once" } as never)),
    ).rejects.toThrow()
    expect(permissionReplies).toEqual([])
    await Effect.runPromise(authorities.permission.reply({ requestID: "per_bound", reply: "once" } as never))
    expect(permissionReplies).toEqual(["per_bound"])

    await expect(
      Effect.runPromise(authorities.question.reply({ requestID: "que_foreign", answers: [] } as never)),
    ).rejects.toThrow()
    await expect(Effect.runPromise(authorities.question.reject("que_foreign" as never))).rejects.toThrow()
    expect(questionReplies).toEqual([])
    await Effect.runPromise(authorities.question.reply({ requestID: "que_bound", answers: [] } as never))
    await Effect.runPromise(authorities.question.reject("que_bound" as never))
    expect(questionReplies).toEqual(["que_bound", "que_bound"])

    // An unregistered worker has no authority at all: every ask is refused,
    // reads resolve to no data, and nothing reaches the hub services.
    const unbound = hubProxyAuthoritiesFor({
      run: (effect) => Effect.runPromise(effect),
      boundSessionID: undefined,
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus,
      permission,
      question,
    })
    const asksBefore = { permission: permissionAsks.length, question: questionAsks.length }
    await expect(Effect.runPromise(unbound.permission.ask(permissionAsk(BOUND)))).rejects.toThrow()
    await expect(Effect.runPromise(unbound.question.ask(questionAsk(BOUND)))).rejects.toThrow()
    expect(permissionAsks).toHaveLength(asksBefore.permission)
    expect(questionAsks).toHaveLength(asksBefore.question)
    expect(await Effect.runPromise(unbound.sessionStatus.get(BOUND))).toEqual({ type: "idle" })
    expect([...(await Effect.runPromise(unbound.sessionStatus.list())).keys()]).toEqual([])
    expect(await Effect.runPromise(unbound.permission.list())).toEqual([])
    expect(await Effect.runPromise(unbound.question.list())).toEqual([])

    // The child-scope RPCs are bound the same way: an unregistered worker can
    // never claim a session by putting it on the request.
    const fakeSessions = {
      get: (id: SessionID) => Effect.succeed({ id, parentID: undefined, title: "fake" } as never),
      children: () => Effect.succeed([]),
      create: (input: object) => Effect.succeed({ id: SessionID.make("ses_fake_child"), ...input } as never),
    } as unknown as Session.Interface
    const fakePrompt = {
      cancel: (sessionID: SessionID) => Effect.succeed(cancelOutcome(sessionID)),
    } as unknown as SessionPromptService.Interface
    const boundless = hubProxyAuthoritiesFor({
      run: (effect) =>
        Effect.runPromise(
          effect.pipe(
            Effect.provideService(Session.Service, fakeSessions),
            Effect.provideService(SessionPromptService.Service, fakePrompt),
          ),
        ),
      boundSessionID: undefined,
      subagentDepth: 1,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus,
      permission,
      question,
    })
    await expect(
      boundless.sessionCreate!({ sessionID: BOUND, parentID: BOUND, title: "unbound child" }),
    ).rejects.toThrow(/no bound session/)
    await expect(boundless.sessionGet!({ sessionID: BOUND, targetID: BOUND })).rejects.toThrow(/no bound session/)
    await expect(boundless.sessionChildren!({ sessionID: BOUND, parentID: BOUND })).rejects.toThrow(/no bound session/)
    await expect(boundless.sessionCancel!({ sessionID: BOUND, targetID: BOUND })).rejects.toThrow(/no bound session/)
  })
})
