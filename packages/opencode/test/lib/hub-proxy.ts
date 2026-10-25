// Shared hub-proxy test fixtures.
//
// A hub-proxy test needs two ends of a `MessageChannel`: the hub-side handler
// map served by `serveHubProxy`, and the worker-side `createHubProxyClient`.
// Several suites spelled that wiring out by hand (and an untyped runner cast the
// effect through `unknown`); this module is the single source.
//
// `instantRunner` is a real `HubProxyRunner`: it runs the authority effect and
// resolves with its value. `serveHubProxyFor` owns the channel and disposes both
// ends together.

import { Effect } from "effect"
import type { HubProxyRunner } from "@/session/worker/hub"
import {
  createHubProxyClient,
  serveHubProxy,
  type HubProxyClient,
  type HubProxyHandlers,
} from "@/session/worker/layer"
import type { HubProxyAuthorities } from "@/session/worker/hub"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SessionStatus } from "@/session/status"

/** Run a hub authority effect on the current runtime. */
export const instantRunner: HubProxyRunner = (effect) => Effect.runPromise(effect)

/** A served hub-proxy pair: the worker-side client plus a combined disposer. */
export interface ServedHubProxy {
  readonly client: HubProxyClient
  /** Stop serving, dispose the client, and close both channel ports. */
  dispose(): void
}

/** Serve `handlers` on a fresh channel and return the client end. */
export function serveHubProxyFor(handlers: HubProxyHandlers): ServedHubProxy {
  const channel = new MessageChannel()
  const disposeServer = serveHubProxy(handlers, channel.port2)
  const client = createHubProxyClient(channel.port1)
  return {
    client,
    dispose() {
      disposeServer()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

/** One authority call observed by a recording stub. */
export interface HubAuthorityCall {
  readonly method: string
  readonly args: ReadonlyArray<unknown>
}

/** Per-authority overrides; anything omitted falls back to an inert default. */
export interface RecordingHubAuthorityOverrides {
  readonly sessionStatus?: Partial<SessionStatus.Interface>
  readonly permission?: Partial<Permission.Interface>
  readonly question?: Partial<Question.Interface>
  readonly dispatchSubagent?: HubProxyAuthorities["dispatchSubagent"]
  readonly systemTransform?: HubProxyAuthorities["systemTransform"]
  readonly messagesTransform?: HubProxyAuthorities["messagesTransform"]
  readonly pluginTool?: HubProxyAuthorities["pluginTool"]
}

export interface RecordingHubAuthorities {
  readonly authorities: HubProxyAuthorities
  /** Every authority call, in order (only the methods actually invoked). */
  readonly calls: ReadonlyArray<HubAuthorityCall>
}

const idleSessionStatus: SessionStatus.Interface = {
  get: () => Effect.succeed({ type: "idle" }),
  set: () => Effect.void,
  list: () => Effect.succeed(new Map()),
}

const inertPermission: Permission.Interface = {
  ask: () => Effect.void,
  reply: () => Effect.void,
  list: () => Effect.succeed([]),
}

const inertQuestion: Question.Interface = {
  ask: () => Effect.succeed([]),
  reply: () => Effect.void,
  reject: () => Effect.void,
  list: () => Effect.succeed([]),
}

/**
 * The full hub-authority map with every method implemented, recording each call
 * into `calls`. Tests that only exercise one authority can still hand a complete
 * map to `buildHubProxyHandlers` without casting empty objects.
 */
export function recordingHubAuthorities(
  overrides: RecordingHubAuthorityOverrides = {},
): RecordingHubAuthorities {
  const calls: HubAuthorityCall[] = []
  const sessionStatusImpl = { ...idleSessionStatus, ...overrides.sessionStatus }
  const permissionImpl = { ...inertPermission, ...overrides.permission }
  const questionImpl = { ...inertQuestion, ...overrides.question }

  const sessionStatus: SessionStatus.Interface = {
    get: (sessionID) => {
      calls.push({ method: "sessionStatus.get", args: [sessionID] })
      return sessionStatusImpl.get(sessionID)
    },
    set: (sessionID, status) => {
      calls.push({ method: "sessionStatus.set", args: [sessionID, status] })
      return sessionStatusImpl.set(sessionID, status)
    },
    list: () => {
      calls.push({ method: "sessionStatus.list", args: [] })
      return sessionStatusImpl.list()
    },
  }

  const permission: Permission.Interface = {
    ask: (input) => {
      calls.push({ method: "permission.ask", args: [input] })
      return permissionImpl.ask(input)
    },
    reply: (input) => {
      calls.push({ method: "permission.reply", args: [input] })
      return permissionImpl.reply(input)
    },
    list: () => {
      calls.push({ method: "permission.list", args: [] })
      return permissionImpl.list()
    },
  }

  const question: Question.Interface = {
    ask: (input) => {
      calls.push({ method: "question.ask", args: [input] })
      return questionImpl.ask(input)
    },
    reply: (input) => {
      calls.push({ method: "question.reply", args: [input] })
      return questionImpl.reply(input)
    },
    reject: (requestID) => {
      calls.push({ method: "question.reject", args: [requestID] })
      return questionImpl.reject(requestID)
    },
    list: () => {
      calls.push({ method: "question.list", args: [] })
      return questionImpl.list()
    },
  }

  const dispatchSubagent = overrides.dispatchSubagent
  const systemTransform = overrides.systemTransform
  const messagesTransform = overrides.messagesTransform
  const pluginTool = overrides.pluginTool
  const authorities: HubProxyAuthorities = {
    sessionStatus,
    permission,
    question,
    ...(dispatchSubagent === undefined
      ? {}
      : {
          dispatchSubagent: (request: Parameters<NonNullable<typeof dispatchSubagent>>[0]) => {
            calls.push({ method: "dispatchSubagent", args: [request] })
            return dispatchSubagent(request)
          },
        }),
    ...(systemTransform === undefined
      ? {}
      : {
          systemTransform: (
            input: Parameters<NonNullable<typeof systemTransform>>[0],
            output: Parameters<NonNullable<typeof systemTransform>>[1],
          ) => {
            calls.push({ method: "systemTransform", args: [input, output] })
            return systemTransform(input, output)
          },
        }),
    // Always present: absent an override it records the call and returns `output`
    // unchanged, so tests need no `buildHubProxyHandlers` pass-through special case.
    messagesTransform: (
      input: Parameters<NonNullable<typeof messagesTransform>>[0],
      output: Parameters<NonNullable<typeof messagesTransform>>[1],
    ) => {
      calls.push({ method: "messagesTransform", args: [input, output] })
      return messagesTransform === undefined ? Promise.resolve(output) : messagesTransform(input, output)
    },
    // Unlike the transforms there is no identity fallback: an execute with no
    // authority must reject, so this key is only present when an override is.
    ...(pluginTool === undefined
      ? {}
      : {
          pluginTool: (input: Parameters<NonNullable<typeof pluginTool>>[0]) => {
            calls.push({ method: "pluginTool.execute", args: [input] })
            return pluginTool(input)
          },
        }),
  }

  return { authorities, calls }
}
