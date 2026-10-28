import { describe, expect, spyOn, test } from "bun:test"
import { Effect } from "effect"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { SESSION_ALLOWLIST, SESSION_EXCLUDED } from "@/session/worker/node-table"
import {
  lspBoundaryLayer,
  mcpBoundaryLayer,
  permissionProxyLayer,
  questionProxyLayer,
  type HubProxyClient,
} from "@/session/worker/layer"
import { LSP } from "@/lsp/lsp"
import { MCP } from "@/mcp"
import * as LSPServer from "@/lsp/server"
import type { QuestionID } from "@/question/schema"

const boundaryOf = (key: string) =>
  [...SESSION_ALLOWLIST, ...SESSION_EXCLUDED].find((entry) => entry.key === key)?.boundary

const recordingClient = () => {
  const calls: Array<{ method: string; args: readonly unknown[] }> = []
  const client: HubProxyClient = {
    call: (method, args) => {
      calls.push({ method, args })
      return Promise.resolve(undefined)
    },
    dispose: () => {},
  }
  return { calls, client }
}

describe("hub-owned permission and question state", () => {
  test("the node table classifies both authorities as hub-proxy", () => {
    expect(boundaryOf(Permission.Service.key)).toBe("hub-proxy")
    expect(boundaryOf(Question.Service.key)).toBe("hub-proxy")
  })

  test("the worker permission service forwards every mutation over the hub port", async () => {
    const { calls, client } = recordingClient()
    await Effect.runPromise(
      Effect.gen(function* () {
        const permission = yield* Permission.Service
        yield* permission.ask({ sessionID: "ses_1", permission: "bash", patterns: ["*"] } as never)
        yield* permission.reply({ requestID: "req_1", reply: "once" } as never)
        yield* permission.list()
      }).pipe(Effect.provide(permissionProxyLayer(client))),
    )
    expect(calls.map((call) => call.method)).toEqual(["permission.ask", "permission.reply", "permission.list"])
  })

  test("the worker question service forwards every mutation over the hub port", async () => {
    const { calls, client } = recordingClient()
    await Effect.runPromise(
      Effect.gen(function* () {
        const question = yield* Question.Service
        yield* question.ask({ sessionID: "ses_1", questions: [] } as never)
        yield* question.reply({ requestID: "que_1" as QuestionID, answers: [] } as never)
        yield* question.reject("que_1" as QuestionID)
        yield* question.list()
      }).pipe(Effect.provide(questionProxyLayer(client))),
    )
    expect(calls.map((call) => call.method)).toEqual([
      "question.ask",
      "question.reply",
      "question.reject",
      "question.list",
    ])
  })
})

describe("MCP/LSP are hub-only in the worker graph", () => {
  test("MCP, LSP and their child-process dependencies are excluded from session workers", () => {
    expect(boundaryOf("@opencode/MCP")).toBe("excluded")
    expect(boundaryOf("@opencode/LSP")).toBe("excluded")
    expect(boundaryOf("@opencode/McpAuth")).toBe("excluded")
    expect(boundaryOf("@opencode/McpBrowser")).toBe("excluded")
  })
})

describe("a worker never spawns MCP/LSP children", () => {
  test("the worker LSP boundary answers without spawning a language server", async () => {
    const spawn = spyOn(LSPServer.Typescript, "spawn")
    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const lsp = yield* LSP.Service
          yield* lsp.init()
          return { status: yield* lsp.status(), diagnostics: yield* lsp.diagnostics() }
        }).pipe(Effect.provide(lspBoundaryLayer())),
      )
      expect(result).toEqual({ status: [], diagnostics: {} })
      expect(spawn).not.toHaveBeenCalled()
    } finally {
      spawn.mockRestore()
    }
  })

  test("the worker MCP boundary answers with an empty catalog", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const mcp = yield* MCP.Service
        return { status: yield* mcp.status(), tools: yield* mcp.tools(), clients: yield* mcp.clients() }
      }).pipe(Effect.provide(mcpBoundaryLayer())),
    )
    expect(result).toEqual({ status: {}, tools: {}, clients: {} })
  })
})
