// Typed-repository hub authority acceptance.
//
// A REAL production hub (`Session`, `SessionProjector`, `EventV2Bridge`,
// `BackgroundJob`, `RuntimeFlags`, `Database`) backs `hubProxyAuthoritiesFor` and
// `sessionSpawnFor`. The worker is represented by its registered bound session.
//
// Acceptance:
//   * `metadata.depth` is hub-owned: a forged request depth never buys headroom,
//     and the persisted child depth is derived from the persisted parent;
//   * a grandchild cannot be spawned, read, listed, or cancelled by a parent-bound
//     worker; direct children still can;
//   * permission/question/status asks, reads, lists, replies, and rejects are
//     bound to the registered session and never reach the underlying service for
//     a foreign or unbound id.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { afterEach, describe, expect, test } from "bun:test"
import { Context, Effect, Exit, Layer, Scope } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { openConnection } from "@opencode-ai/core/database/connection"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { BackgroundJob } from "@/background/job"
import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceRef } from "@/effect/instance-ref"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { SessionPromptService } from "@/session/prompt-service"
import { MessageID, SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { markSchemaIdentity } from "@/session/db/schema"
import { migrationStatements } from "@/session/writer/migration.statements.gen"
import {
  createSessionChildrenIndex,
  hubProxyAuthoritiesFor,
  sessionSpawnFor,
  type HubProxyRunner,
} from "@/session/worker/hub"
import { instantRunner, recordingHubAuthorities } from "../lib/hub-proxy"

const ROOT = SessionID.make("ses_final_root")
const PARENT = SessionID.make("ses_final_parent")
const DEEP = SessionID.make("ses_final_deep")
const PROJECT = "prj_final_hub"
const DIRECTORIES: string[] = []
const CLEANUPS: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-final-hub-"))
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

function seedSession(
  filename: string,
  directory: string,
  id: SessionID,
  parentID: SessionID | undefined,
  depth: number,
): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO session (id, project_id, slug, directory, title, version, parent_id, metadata, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [
        String(id),
        PROJECT,
        String(id),
        directory,
        String(id),
        "1",
        parentID === undefined ? null : String(parentID),
        JSON.stringify({ depth }),
        1,
        1,
      ],
    )
  } finally {
    connection.close()
  }
}

function seedSessions(filename: string, directory: string): void {
  const connection = openConnection({ filename })
  try {
    connection.run(
      "INSERT INTO project (id, name, worktree, sandboxes, time_created, time_updated) VALUES (?, ?, ?, ?, ?, ?)",
      [PROJECT, "final-hub", directory, "[]", 1, 1],
    )
  } finally {
    connection.close()
  }
  seedSession(filename, directory, ROOT, undefined, 0)
  seedSession(filename, directory, PARENT, ROOT, 1)
  seedSession(filename, directory, DEEP, ROOT, 2)
}

function metadataOf(filename: string, id: SessionID): Record<string, unknown> {
  const connection = openConnection({ filename })
  try {
    const raw = connection.all<{ metadata: string | null }>("SELECT metadata FROM session WHERE id = ?", [
      String(id),
    ])[0]?.metadata
    return raw === null || raw === undefined ? {} : (JSON.parse(raw) as Record<string, unknown>)
  } finally {
    connection.close()
  }
}

function childReply(input: SessionPromptService.PromptInput): SessionV1.WithParts {
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
        id: "part-final-reply" as never,
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text: "child replied",
      },
    ],
  }
}

function recordingPrompt(prompts: SessionPromptService.PromptInput[], cancels: SessionID[]): SessionPromptService.Interface {
  return {
    prompt: (input: SessionPromptService.PromptInput) =>
      Effect.sync(() => {
        prompts.push(input)
        return childReply(input)
      }),
    cancel: (sessionID: SessionID) =>
      Effect.sync(() => {
        cancels.push(sessionID)
        return { sessionID, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }
      }),
  } as unknown as SessionPromptService.Interface
}

async function openHub(
  filename: string,
  directory: string,
  prompt: SessionPromptService.Interface,
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

interface Outcome {
  readonly ok: boolean
  readonly value?: unknown
  readonly error?: unknown
}

async function settle(operation: () => Promise<unknown> | unknown): Promise<Outcome> {
  try {
    return { ok: true, value: await operation() }
  } catch (error) {
    return { ok: false, error }
  }
}

function spawnRequest(parentID: SessionID, metadataDepth: number, childID?: SessionID) {
  return {
    parentID,
    ...(childID === undefined ? {} : { childID }),
    type: "task",
    metadata: { depth: metadataDepth },
    prompt: {
      messageID: MessageID.ascending(),
      sessionID: childID ?? parentID,
      parts: [{ type: "text", text: "final spawn probe" }],
    },
  } as never
}

afterEach(async () => {
  for (const cleanup of CLEANUPS.splice(0).reverse()) await cleanup()
  for (const directory of DIRECTORIES.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("metadata.depth is hub-owned and grandchild authority is absent", () => {
  test("forged depth cannot buy headroom; persisted depth comes from the parent", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedSessions(filename, directory)

    const prompts: SessionPromptService.PromptInput[] = []
    const cancels: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, recordingPrompt(prompts, cancels))
    const depths = new Map<SessionID, number>()
    const edges = createSessionChildrenIndex()
    const deps = {
      run,
      subagentDepth: 2,
      recordDepth: (sessionID: SessionID, depth: number) => {
        if (depth > 0) depths.set(sessionID, depth)
        else depths.delete(sessionID)
      },
      registerChild: edges.register,
    }

    // DEEP is already at the cap (depth 2). Trusting request depth 0 would create
    // a depth-1 child and then allow a grandchild.
    await expect(sessionSpawnFor(DEEP, deps)(spawnRequest(DEEP, 0))).rejects.toThrow(
      /Subagent depth limit reached \(2\)/,
    )
    expect(await run(sessions.children(DEEP))).toEqual([])
    expect(prompts).toHaveLength(0)
    expect(depths.size).toBe(0)
    expect(edges.childrenOf(DEEP)).toEqual([])

    // Below the cap the spawn succeeds, but the child carries parent depth + 1.
    const child = await sessionSpawnFor(PARENT, deps)(spawnRequest(PARENT, 0))
    expect(metadataOf(filename, child.childID).depth).toBe(2)
    expect(depths.get(child.childID)).toBe(2)
    expect(edges.childrenOf(PARENT)).toEqual([child.childID])
    expect(prompts.map((input) => String(input.sessionID))).toEqual([String(child.childID)])

    // The depth-2 child is now at the cap: its worker cannot spawn a grandchild.
    await expect(
      sessionSpawnFor(child.childID, { ...deps, registerChild: () => {} })(spawnRequest(child.childID, 0)),
    ).rejects.toThrow(/Subagent depth limit reached \(2\)/)
    expect(await run(sessions.children(child.childID))).toEqual([])
    expect(metadataOf(filename, child.childID).depth).toBe(2)

    // A forged depth 0 in session.create is likewise replaced by parent depth + 1.
    const base = recordingHubAuthorities()
    const authorities = hubProxyAuthoritiesFor({
      ...deps,
      boundSessionID: PARENT,
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })
    const forged = await authorities.sessionCreate!({
      sessionID: PARENT,
      parentID: PARENT,
      title: "forged create",
      metadata: { depth: 0, keep: "caller data" },
    })
    expect(metadataOf(filename, forged.id)).toMatchObject({ depth: 2, keep: "caller data" })
  }, 60_000)

  test("grandchild spawn, read, children, and cancel are rejected; direct children work", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedSessions(filename, directory)

    const prompts: SessionPromptService.PromptInput[] = []
    const cancels: SessionID[] = []
    const { sessions, run } = await openHub(filename, directory, recordingPrompt(prompts, cancels))
    const base = recordingHubAuthorities()
    const authorities = hubProxyAuthoritiesFor({
      run,
      boundSessionID: PARENT,
      subagentDepth: 3,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })

    const child = await run(sessions.create({ parentID: PARENT, title: "direct child" }))
    const grandchild = await run(sessions.create({ parentID: child.id, title: "grandchild" }))
    const foreign = await run(sessions.create({ title: "foreign" }))

    // Direct child: readable, listable, cancellable.
    const read = await authorities.sessionGet!({ sessionID: PARENT, targetID: child.id })
    expect(read).not.toBeNull()
    expect(String(read!.id)).toBe(String(child.id))
    const listed = await authorities.sessionChildren!({ sessionID: PARENT, parentID: PARENT })
    expect(listed.map((item) => String(item.id))).toContain(String(child.id))
    expect(await authorities.sessionCancel!({ sessionID: PARENT, targetID: child.id })).toBeDefined()

    // Self, grandchild, and foreign ids are outside the bound parent's scope.
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: PARENT })).rejects.toThrow(/not a direct child/)
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: grandchild.id })).rejects.toThrow(
      /not a direct child/,
    )
    await expect(authorities.sessionChildren!({ sessionID: PARENT, parentID: child.id })).rejects.toThrow(
      /may not list children/,
    )
    await expect(authorities.sessionCancel!({ sessionID: PARENT, targetID: grandchild.id })).rejects.toThrow(
      /not a direct child/,
    )
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: foreign.id })).rejects.toThrow(
      /not a direct child/,
    )
    await expect(authorities.sessionGet!({ sessionID: PARENT, targetID: ROOT })).rejects.toThrow(/not a direct child/)
    await expect(authorities.sessionCreate!({ sessionID: PARENT, parentID: ROOT, title: "not mine" })).rejects.toThrow(
      /may not create a child under/,
    )
    await expect(authorities.sessionSpawn!(spawnRequest(PARENT, 0, grandchild.id))).rejects.toThrow(
      /not a direct child/,
    )
    await expect(authorities.sessionSpawn!(spawnRequest(PARENT, 0, PARENT))).rejects.toThrow(
      /resume its own session as a child/,
    )
    expect(cancels).toEqual([child.id])
  }, 60_000)

  test("an unbound worker cannot establish session authority from the request", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedSessions(filename, directory)

    const { run } = await openHub(filename, directory, recordingPrompt([], []))
    const base = recordingHubAuthorities()
    const unbound = hubProxyAuthoritiesFor({
      run,
      boundSessionID: undefined,
      subagentDepth: 2,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })

    await expect(
      unbound.sessionCreate!({ sessionID: PARENT, parentID: PARENT, title: "unbound child" }),
    ).rejects.toThrow(/no bound session/)
    await expect(unbound.sessionGet!({ sessionID: PARENT, targetID: PARENT })).rejects.toThrow(/no bound session/)
    await expect(unbound.sessionChildren!({ sessionID: PARENT, parentID: PARENT })).rejects.toThrow(/no bound session/)
    await expect(unbound.sessionCancel!({ sessionID: PARENT, targetID: PARENT })).rejects.toThrow(/no bound session/)
    await expect(unbound.sessionSpawn!(spawnRequest(PARENT, 0))).rejects.toThrow(/no bound session/)
  }, 60_000)
})

describe("permission, question, and status are bound to the registered session", () => {
  test("foreign and unbound ids never reach the underlying service", async () => {
    const BOUND = SessionID.make("ses_final_bound")
    const FOREIGN = SessionID.make("ses_final_foreign")
    const statuses = new Map<SessionID, { type: "busy" | "idle" }>([
      [BOUND, { type: "busy" }],
      [FOREIGN, { type: "busy" }],
    ])
    const statusWrites: string[] = []
    const permissionRequests = [
      { id: "per_bound", sessionID: BOUND, permission: "read", patterns: ["*"], metadata: {}, always: [] },
      { id: "per_foreign", sessionID: FOREIGN, permission: "read", patterns: ["*"], metadata: {}, always: [] },
    ]
    const questionRequests = [
      { id: "que_bound", sessionID: BOUND, questions: [] },
      { id: "que_foreign", sessionID: FOREIGN, questions: [] },
    ]
    const replies: string[] = []
    const base = recordingHubAuthorities({
      permission: {
        ask: () => Effect.void,
        list: () => Effect.succeed(permissionRequests as never),
        reply: (input) =>
          Effect.sync(() => {
            replies.push(`permission:${String(input.requestID)}`)
          }),
      },
      question: {
        ask: () => Effect.succeed([]),
        list: () => Effect.succeed(questionRequests as never),
        reply: (input) =>
          Effect.sync(() => {
            replies.push(`question.reply:${String(input.requestID)}`)
          }),
        reject: (requestID) =>
          Effect.sync(() => {
            replies.push(`question.reject:${String(requestID)}`)
          }),
      },
      sessionStatus: {
        get: (sessionID) => Effect.succeed(statuses.get(sessionID) ?? { type: "idle" }),
        list: () => Effect.succeed(new Map(statuses)),
        set: (sessionID, status) =>
          Effect.sync(() => {
            statusWrites.push(`${String(sessionID)}:${status.type}`)
          }),
      },
    })

    const authorities = hubProxyAuthoritiesFor({
      run: instantRunner,
      boundSessionID: BOUND,
      subagentDepth: 2,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })

    const permissionAsk = (sessionID: SessionID) =>
      ({ sessionID, permission: "read", patterns: ["*"], metadata: {}, always: [], ruleset: [] }) as never
    const questionAsk = (sessionID: SessionID) => ({ sessionID, questions: [] }) as never

    await expect(Effect.runPromise(authorities.permission.ask(permissionAsk(FOREIGN)))).rejects.toThrow()
    await expect(Effect.runPromise(authorities.question.ask(questionAsk(FOREIGN)))).rejects.toThrow()
    expect(base.calls.some((call) => call.method === "permission.ask" && call.args.length > 0)).toBe(false)
    expect(base.calls.some((call) => call.method === "question.ask" && call.args.length > 0)).toBe(false)

    await Effect.runPromise(authorities.permission.ask(permissionAsk(BOUND)))
    await Effect.runPromise(authorities.question.ask(questionAsk(BOUND)))
    expect(base.calls.filter((call) => call.method === "permission.ask")).toHaveLength(1)
    expect(base.calls.filter((call) => call.method === "question.ask")).toHaveLength(1)

    // Foreign reply/reject fails without reaching the inner service.
    await expect(
      Effect.runPromise(authorities.permission.reply({ requestID: "per_foreign", reply: "once" } as never)),
    ).rejects.toThrow()
    await expect(
      Effect.runPromise(authorities.question.reply({ requestID: "que_foreign", answers: [] } as never)),
    ).rejects.toThrow()
    await expect(Effect.runPromise(authorities.question.reject("que_foreign" as never))).rejects.toThrow()
    expect(replies).toEqual([])

    await Effect.runPromise(authorities.permission.reply({ requestID: "per_bound", reply: "once" } as never))
    await Effect.runPromise(authorities.question.reply({ requestID: "que_bound", answers: [] } as never))
    await Effect.runPromise(authorities.question.reject("que_bound" as never))
    expect(replies).toEqual(["permission:per_bound", "question.reply:que_bound", "question.reject:que_bound"])

    // Foreign status reads/writes are inert; lists expose only the bound entry.
    expect(await Effect.runPromise(authorities.sessionStatus.get(FOREIGN))).toEqual({ type: "idle" })
    expect(await Effect.runPromise(authorities.sessionStatus.get(BOUND))).toEqual({ type: "busy" })
    const listedStatus = await Effect.runPromise(authorities.sessionStatus.list())
    expect([...listedStatus.keys()].map(String)).toEqual([String(BOUND)])
    await Effect.runPromise(authorities.sessionStatus.set(FOREIGN, { type: "idle" } as never))
    expect(statusWrites).toEqual([])
    await Effect.runPromise(authorities.sessionStatus.set(BOUND, { type: "idle" } as never))
    expect(statusWrites).toEqual([`${String(BOUND)}:idle`])

    const listedPermission = await Effect.runPromise(authorities.permission.list())
    expect((listedPermission as ReadonlyArray<{ id: string }>).map((item) => item.id)).toEqual(["per_bound"])
    const listedQuestion = await Effect.runPromise(authorities.question.list())
    expect((listedQuestion as ReadonlyArray<{ id: string }>).map((item) => item.id)).toEqual(["que_bound"])

    // Unbound is inert for every read/list and rejects every ask.
    const unbound = hubProxyAuthoritiesFor({
      run: instantRunner,
      boundSessionID: undefined,
      subagentDepth: 2,
      recordDepth: () => {},
      registerChild: () => {},
      sessionStatus: base.authorities.sessionStatus,
      permission: base.authorities.permission,
      question: base.authorities.question,
    })
    await expect(Effect.runPromise(unbound.permission.ask(permissionAsk(BOUND)))).rejects.toThrow()
    await expect(Effect.runPromise(unbound.question.ask(questionAsk(BOUND)))).rejects.toThrow()
    expect(await Effect.runPromise(unbound.sessionStatus.get(BOUND))).toEqual({ type: "idle" })
    expect([...(await Effect.runPromise(unbound.sessionStatus.list())).keys()]).toEqual([])
    expect(await Effect.runPromise(unbound.permission.list())).toEqual([])
    expect(await Effect.runPromise(unbound.question.list())).toEqual([])
  }, 60_000)
})
