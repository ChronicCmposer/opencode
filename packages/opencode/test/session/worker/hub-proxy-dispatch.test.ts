// Solution 1 (B1) — the hub-proxy `dispatchSubagent` RPC.
//
// A session worker has no `SessionHubService`, so `SessionPrompt.ops.dispatchSubagent`
// reaches the hub over the existing multiplexed control port. This suite pins the
// wire contract:
//   * the multiplexed client exposes `dispatchSubagent` and returns the settled
//     `JobInfo` the hub produced;
//   * domain outcomes (headroom-full, depth, cancel) come back as a
//     STATUS-BEARING `JobInfo`, never a rejection;
//   * only a transport fault (an unserved method) rejects;
//   * end-to-end through `createSessionHub`, the child's last assistant text is
//     bridged into `JobInfo.output`.

import { describe, expect, test } from "bun:test"
import type { SessionID } from "@/session/schema"
import { buildHubProxyHandlers, createSessionHub, type HubProxyAuthorities } from "@/session/worker/hub"
import { createHubProxyClient, serveHubProxy } from "@/session/worker/layer"
import { createJobRegistry, type JobInfo } from "@/session/worker/job-registry"
import { dispatchSubagent, type SubagentDispatchRequest } from "@/session/worker/subagent"
import { SessionCancelledError } from "@/session/worker/dispatch"
import {
  createExecutorWorkerPool,
  type ExecutorRun,
  type ExecutorWorker,
  type SessionExecutionResult,
} from "@/session/worker/execute"
import { capture, flush, sid } from "../../lib/async"

const instantRunner = (effect: unknown): Promise<never> => Promise.resolve(effect as never)

/** A settled `JobInfo` as the hub hands it back across the port. */
function settledJob(id: string, status: JobInfo["status"]): JobInfo {
  return {
    id,
    type: "task",
    status,
    started_at: 0,
    completed_at: 1,
    depth: 1,
    ...(status === "completed" ? { output: "done" } : {}),
    ...(status === "error" ? { error: "refused" } : {}),
  }
}

/** The authority map `buildHubProxyHandlers` accepts, with only dispatch wired. */
function authorities(dispatchSubagent?: (request: SubagentDispatchRequest) => Promise<JobInfo>): HubProxyAuthorities {
  return {
    sessionStatus: {} as never,
    permission: {} as never,
    question: {} as never,
    ...(dispatchSubagent === undefined ? {} : { dispatchSubagent }),
  }
}

/** Serve `handlers` on a fresh channel and return the client end. */
function clientFor(handlers: Parameters<typeof serveHubProxy>[0]): ReturnType<typeof createHubProxyClient> {
  const channel = new MessageChannel()
  serveHubProxy(handlers, channel.port2)
  return createHubProxyClient(channel.port1)
}

describe("hub-proxy dispatchSubagent RPC (B1)", () => {
  test("the multiplexed client exposes dispatchSubagent and returns the settled JobInfo", async () => {
    const client = clientFor(buildHubProxyHandlers(authorities(async (request) => settledJob(request.childID, "completed")), instantRunner))

    expect(typeof client.dispatchSubagent).toBe("function")
    const info = await client.dispatchSubagent!({
      parentID: sid("ses_parent"),
      childID: sid("ses_child"),
      type: "task",
      depth: 1,
      prompt: { sessionID: sid("ses_child"), agent: "build", parts: [{ type: "text", text: "go" }] },
    })
    expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "done" })
    client.dispose()
  })

  test("a depth refusal returns a status-bearing JobInfo (not a throw)", async () => {
    const registry = createJobRegistry()
    const client = clientFor(
      buildHubProxyHandlers(
        authorities((request) =>
          dispatchSubagent(
            { registry, scheduler: { dispatch: async () => {} }, depthLimit: 0, headroomFull: () => false },
            request,
          ),
        ),
        instantRunner,
      ),
    )

    const settled = await capture(
      client.dispatchSubagent!({ parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task", depth: 1 }),
    )
    expect(settled.ok).toBe(true)
    if (settled.ok) expect(settled.value).toMatchObject({ id: "ses_child", status: "error" })
    expect(registry.get("ses_child")).toBeUndefined()
    client.dispose()
  })

  test("headroom-full comes back as an error JobInfo, a cancel as cancelled", async () => {
    const registry = createJobRegistry()
    const clientForHandler = (authority: (request: SubagentDispatchRequest) => Promise<JobInfo>) =>
      clientFor(buildHubProxyHandlers(authorities(authority), instantRunner))

    const full = clientForHandler((request) =>
      dispatchSubagent(
        { registry, scheduler: { dispatch: async () => {} }, depthLimit: 1, headroomFull: () => true },
        request,
      ),
    )
    const headroom = await full.dispatchSubagent!({ parentID: sid("ses_parent"), childID: sid("ses_a"), type: "task", depth: 1 })
    expect(headroom.status).toBe("error")

    const cancelling = clientForHandler((request) =>
      dispatchSubagent(
        {
          registry,
          scheduler: {
            dispatch: () => Promise.reject(new SessionCancelledError({ message: "stop", sessionID: sid("ses_b") })),
          },
          depthLimit: 1,
          headroomFull: () => false,
        },
        request,
      ),
    )
    const cancelled = await cancelling.dispatchSubagent!({
      parentID: sid("ses_parent"),
      childID: sid("ses_b"),
      type: "task",
      depth: 1,
    })
    expect(cancelled.status).toBe("cancelled")

    full.dispose()
    cancelling.dispose()
  })

  test("an unserved method is the only rejection (transport error)", async () => {
    const client = clientFor({})

    const settled = await capture(
      client.dispatchSubagent!({ parentID: sid("ses_parent"), childID: sid("ses_child"), type: "task", depth: 1 }),
    )
    expect(settled.ok).toBe(false)
    if (!settled.ok) expect(String(settled.error)).toContain("unknown hub-proxy method")
    client.dispose()
  })
})

// ---------------------------------------------------------------------------
// End-to-end through `createSessionHub`: the hub serves the RPC, dispatches the
// child to a worker with its prompt, and bridges the worker's last assistant
// text into `JobInfo.output`.
// ---------------------------------------------------------------------------

/** An in-memory executor worker that completes instantly and keeps its result. */
class BridgingWorker implements ExecutorWorker {
  alive = true
  readonly runs: ExecutorRun[] = []
  private readonly results = new Map<SessionID, SessionExecutionResult>()

  constructor(readonly id: number) {}

  async execute(run: ExecutorRun): Promise<void> {
    this.runs.push(run)
    this.results.set(run.sessionID, {
      sessionID: run.sessionID,
      value: {
        info: { role: "assistant" },
        parts: [
          { type: "text", text: "thinking..." },
          { type: "text", text: "the final answer" },
        ],
      },
    })
  }
  async cancel(): Promise<void> {}
  async assertNotBusy(): Promise<boolean> {
    return false
  }
  resultFor(sessionID: SessionID): SessionExecutionResult | undefined {
    return this.results.get(sessionID)
  }
  async terminate(): Promise<void> {
    this.alive = false
  }
}

describe("hub serves dispatchSubagent and bridges final text (B1)", () => {
  test("a depth-1 child runs with its prompt and its output lands in JobInfo.output", async () => {
    const spawned: BridgingWorker[] = []
    const pool = createExecutorWorkerPool({
      k: 1,
      spawn: (id) => {
        const worker = new BridgingWorker(id)
        spawned.push(worker)
        return worker
      },
    })
    const hub = createSessionHub({
      pool,
      subagentDepth: 2,
      ports: { writer: {} as never, broker: {} as never, hubProxy: {} },
    })
    const channel = new MessageChannel()
    const dispose = hub.attachHubProxyPort(1, channel.port1)
    const client = createHubProxyClient(channel.port2)

    const prompt = { sessionID: sid("ses_child"), agent: "build", parts: [{ type: "text" as const, text: "do it" }] }
    const info = await client.dispatchSubagent!({
      parentID: sid("ses_parent"),
      childID: sid("ses_child"),
      type: "task",
      depth: 1,
      prompt,
    })
    await flush()

    expect(info).toMatchObject({ id: "ses_child", status: "completed", output: "the final answer", depth: 1 })
    expect(hub.jobs.get("ses_child")?.output).toBe("the final answer")
    // The prompt rode the dispatch to the headroom worker.
    const childWorker = spawned.find((worker) => worker.runs.length > 0)
    expect(childWorker?.runs).toEqual([{ sessionID: sid("ses_child"), depth: 1, prompt }])

    dispose()
    client.dispose()
    await hub.terminateAll()
  })

  test("headroom-full over the port is status-bearing, never a throw", async () => {
    const pool = createExecutorWorkerPool({ k: 1, spawn: (id) => new BridgingWorker(id) })
    const hub = createSessionHub({
      pool,
      subagentDepth: 1,
      // A zero-capacity headroom pool makes the admission check trivially full.
      headroom: 0,
      ports: { writer: {} as never, broker: {} as never, hubProxy: {} },
    })
    const channel = new MessageChannel()
    const dispose = hub.attachHubProxyPort(1, channel.port1)
    const client = createHubProxyClient(channel.port2)

    const info = await client.dispatchSubagent!({
      parentID: sid("ses_parent"),
      childID: sid("ses_child"),
      type: "task",
      depth: 1,
    })
    expect(info.status).toBe("error")
    expect(hub.jobs.get("ses_child")).toBeUndefined()

    dispose()
    client.dispose()
    await hub.terminateAll()
  })
})
