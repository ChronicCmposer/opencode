// Hub-authority half of the typed-repository matrix.
//
// A REAL production hub (`Session`, `SessionProjector`, `EventV2Bridge`,
// `Database`) backs `hubProxyAuthoritiesFor`/`sessionSpawnFor`. The worker is
// bound to one session; a forged `metadata.depth` must not buy a grandchild, and
// foreign permission/question/status ids must never reach the underlying
// service.

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

const ROOT = SessionID.make("ses_typed_verify_root")
const PARENT = SessionID.make("ses_typed_verify_parent")
const DEEP = SessionID.make("ses_typed_verify_deep")
const PROJECT = "prj_typed_verify"
const DIRECTORIES: string[] = []
const CLEANUPS: Array<() => void | Promise<void>> = []

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-typed-hub-"))
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
      [PROJECT, "typed-verify", directory, "[]", 1, 1],
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
        id: "part-typed-verify-reply" as never,
        messageID: id,
        sessionID: input.sessionID,
        type: "text",
        text: "child replied",
      },
    ],
  }
}

function recordingPrompt(sessions: Session.Interface, prompts: SessionPromptService.PromptInput[]): SessionPromptService.Interface {
  return {
    prompt: (input: SessionPromptService.PromptInput) =>
      Effect.sync(() => {
        prompts.push(input)
        return childReply(input)
      }),
    cancel: () =>
      Effect.succeed({ sessionID: ROOT, mode: "terminated", winner: "cancel", acknowledged: false, durationMs: 0 }),
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
      parts: [{ type: "text", text: "typed-verify spawn probe" }],
    },
  } as never
}

afterEach(async () => {
  for (const cleanup of CLEANUPS.splice(0).reverse()) await cleanup()
  for (const directory of DIRECTORIES.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("hub-owned session depth", () => {
  test("a forged metadata.depth=0 cannot buy a grandchild under a capped parent", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedSessions(filename, directory)

    const prompts: SessionPromptService.PromptInput[] = []
    const { sessions, run } = await openHub(filename, directory, (service) => recordingPrompt(service, prompts))
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

    // The parent is already at the cap (depth 2). Trusting the request's
    // depth 0 would create a depth-1 child and then allow a grandchild.
    await expect(sessionSpawnFor(DEEP, deps)(spawnRequest(DEEP, 0))).rejects.toThrow(
      /Subagent depth limit reached \(2\)/,
    )
    expect(await run(sessions.children(DEEP))).toEqual([])
    expect(prompts).toHaveLength(0)
    expect(depths.size).toBe(0)
    expect(edges.childrenOf(DEEP)).toEqual([])

    // Below the cap the spawn succeeds, but the persisted child carries the
    // derived depth (2), never the caller's 0.
    const child = await sessionSpawnFor(PARENT, deps)(spawnRequest(PARENT, 0))
    expect(metadataOf(filename, child.childID).depth).toBe(2)
    expect(depths.get(child.childID)).toBe(2)
    expect(edges.childrenOf(PARENT)).toEqual([child.childID])
    expect(prompts.map((input) => String(input.sessionID))).toEqual([String(child.childID)])

    // That depth-2 child is now at the cap: a second forged depth 0 must not
    // create a grandchild.
    await expect(
      sessionSpawnFor(child.childID, { ...deps, registerChild: () => {} })(spawnRequest(child.childID, 0)),
    ).rejects.toThrow(/Subagent depth limit reached \(2\)/)
    expect(await run(sessions.children(child.childID))).toEqual([])
    expect(metadataOf(filename, child.childID).depth).toBe(2)
  }, 60_000)

  test("spawn still rejects a foreign childID and an unbound worker", async () => {
    const directory = workspace()
    const filename = path.join(directory, "opencode.db")
    migratedDatabase(filename)
    seedSessions(filename, directory)

    const prompts: SessionPromptService.PromptInput[] = []
    const { sessions, run } = await openHub(filename, directory, (service) => recordingPrompt(service, prompts))
    const deps = {
      run,
      subagentDepth: 2,
      recordDepth: () => {},
      registerChild: () => {},
    }
    const foreign = await run(sessions.create({ title: "foreign" }))

    await expect(sessionSpawnFor(PARENT, deps)(spawnRequest(PARENT, 0, foreign.id))).rejects.toThrow(
      /not a direct child/,
    )

    const unbound = sessionSpawnFor(undefined, deps)
    await expect(unbound(spawnRequest(PARENT, 0))).rejects.toThrow(/no bound session/)
    expect(prompts).toHaveLength(0)
  }, 60_000)
})

describe("hub-bound permission, question, and status", () => {
  test("foreign ask/status ids never reach the service; lists only expose the bound session", async () => {
    const BOUND = SessionID.make("ses_typed_verify_bound")
    const FOREIGN = SessionID.make("ses_typed_verify_foreign")
    const permissionAsked: unknown[] = []
    const questionAsked: unknown[] = []
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

    const base = recordingHubAuthorities({
      permission: {
        ask: (input) =>
          Effect.sync(() => {
            permissionAsked.push(input)
          }),
        list: () => Effect.succeed(permissionRequests as never),
      },
      question: {
        ask: (input) =>
          Effect.sync(() => {
            questionAsked.push(input)
            return []
          }),
        list: () => Effect.succeed(questionRequests as never),
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

    const permissionAsk = (sessionID: SessionID) => ({
      sessionID,
      permission: "read",
      patterns: ["*"],
      metadata: {},
      always: [],
      ruleset: [],
    })

    const foreignPermission = await settle(() =>
      Effect.runPromise(authorities.permission.ask(permissionAsk(FOREIGN) as never)),
    )
    expect(foreignPermission.ok, "a foreign permission.ask reached the service").toBe(false)
    expect(permissionAsked).toEqual([])

    const boundPermission = await settle(() =>
      Effect.runPromise(authorities.permission.ask(permissionAsk(BOUND) as never)),
    )
    expect(boundPermission.ok).toBe(true)
    expect(permissionAsked).toHaveLength(1)

    const questionAsk = (sessionID: SessionID) => ({ sessionID, questions: [] })
    const foreignQuestion = await settle(() =>
      Effect.runPromise(authorities.question.ask(questionAsk(FOREIGN) as never)),
    )
    expect(foreignQuestion.ok, "a foreign question.ask reached the service").toBe(false)
    expect(questionAsked).toEqual([])
    const boundQuestion = await settle(() => Effect.runPromise(authorities.question.ask(questionAsk(BOUND) as never)))
    expect(boundQuestion.ok).toBe(true)
    expect(questionAsked).toHaveLength(1)

    // get(foreign) must not expose foreign state (reject or idle); get(bound)
    // still returns the real status.
    const foreignStatus = await settle(() => Effect.runPromise(authorities.sessionStatus.get(FOREIGN)))
    if (foreignStatus.ok) {
      expect((foreignStatus.value as { type?: string }).type).not.toBe("busy")
    } else {
      expect(foreignStatus.error).toBeDefined()
    }
    const boundStatus = await settle(() => Effect.runPromise(authorities.sessionStatus.get(BOUND)))
    expect(boundStatus.ok).toBe(true)
    expect((boundStatus.value as { type?: string }).type).toBe("busy")

    const listedStatus = await settle(() => Effect.runPromise(authorities.sessionStatus.list()))
    expect(listedStatus.ok).toBe(true)
    const keys = [...(listedStatus.value as Map<SessionID, unknown>).keys()].map(String)
    expect(keys).toEqual([String(BOUND)])

    await Effect.runPromise(authorities.sessionStatus.set(FOREIGN, { type: "idle" }))
    expect(statusWrites).toEqual([])
    await Effect.runPromise(authorities.sessionStatus.set(BOUND, { type: "idle" }))
    expect(statusWrites).toEqual([`${String(BOUND)}:idle`])

    const listedPermission = await settle(() => Effect.runPromise(authorities.permission.list()))
    expect(listedPermission.ok).toBe(true)
    expect((listedPermission.value as ReadonlyArray<{ id: string }>).map((item) => item.id)).toEqual(["per_bound"])
    const listedQuestion = await settle(() => Effect.runPromise(authorities.question.list()))
    expect(listedQuestion.ok).toBe(true)
    expect((listedQuestion.value as ReadonlyArray<{ id: string }>).map((item) => item.id)).toEqual(["que_bound"])
  })
})
