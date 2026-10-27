import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test"
import path from "path"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Effect } from "effect"
import { LSP } from "@/lsp/lsp"
import * as LSPServer from "@/lsp/server"
import { TestInstance } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { LspTool } from "@/tool/lsp"
import { ToolRoute } from "@/tool/route"

const it = testEffect(LayerNode.compile(LSP.node))

const fakeServerPath = path.join(__dirname, "../fixture/lsp/fake-lsp-server.js")

describe("LSP service lifecycle", () => {
  let spawnSpy: ReturnType<typeof spyOn>

  beforeEach(() => {
    spawnSpy = spyOn(LSPServer.Typescript, "spawn").mockResolvedValue(undefined)
  })

  afterEach(() => {
    spawnSpy.mockRestore()
  })

  it.instance("init() completes without error", () => LSP.Service.use((lsp) => lsp.init()))

  it.instance("status() returns empty array initially", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.status()
        expect(Array.isArray(result)).toBe(true)
        expect(result.length).toBe(0)
      }),
    ),
  )

  it.instance("diagnostics() returns empty object initially", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.diagnostics()
        expect(typeof result).toBe("object")
        expect(Object.keys(result).length).toBe(0)
      }),
    ),
  )

  it.instance("hasClients() returns false for .ts files in instance when LSP is unset", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
        expect(result).toBe(false)
      }),
    ),
  )

  it.instance(
    "hasClients() returns true for .ts files in instance when lsp is true",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
          expect(result).toBe(true)
        }),
      ),
    { config: { lsp: true } },
  )

  it.instance(
    "hasClients() keeps built-in LSPs when config object is provided",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "test.ts"))
          expect(result).toBe(true)
        }),
      ),
    { config: { lsp: { eslint: { disabled: true } } } },
  )

  it.instance("hasClients() returns false for files outside instance", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.hasClients(path.join((yield* TestInstance).directory, "..", "outside.ts"))
        expect(typeof result).toBe("boolean")
      }),
    ),
  )

  it.instance("workspaceSymbol() returns empty array with no clients", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.workspaceSymbol("test")
        expect(Array.isArray(result)).toBe(true)
        expect(result.length).toBe(0)
      }),
    ),
  )

  it.instance("definition() returns empty array for unknown file", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.definition({
          file: path.join((yield* TestInstance).directory, "nonexistent.ts"),
          line: 0,
          character: 0,
        })
        expect(Array.isArray(result)).toBe(true)
      }),
    ),
  )

  it.instance("references() returns empty array for unknown file", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        const result = yield* lsp.references({
          file: path.join((yield* TestInstance).directory, "nonexistent.ts"),
          line: 0,
          character: 0,
        })
        expect(Array.isArray(result)).toBe(true)
      }),
    ),
  )

  it.instance("multiple init() calls are idempotent", () =>
    LSP.Service.use((lsp) =>
      Effect.gen(function* () {
        yield* lsp.init()
        yield* lsp.init()
        yield* lsp.init()
      }),
    ),
  )
})

describe("LSP tool parity and child lifecycle", () => {
  test("the LSP tool stays hub-only and is not deleted", () => {
    expect(LspTool.id).toBe("lsp")
    const source = ToolRoute.sourceForRegistryTool("lsp")
    expect(source).toBe("lsp")
    expect(ToolRoute.hostForSource(source)).toBe("hub")
    expect(ToolRoute.runsOn(ToolRoute.routeTool({ id: "lsp", source }), "worker")).toBe(false)
  })

  it.instance(
    "repeated and concurrent first use reuses one language-server client",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const file = path.join(dir, "cardinality.repro")
          yield* Effect.promise(() => Bun.write(file, "sample\n"))

          yield* Effect.all([lsp.touchFile(file), lsp.touchFile(file), lsp.touchFile(file)], {
            concurrency: "unbounded",
          })
          yield* lsp.touchFile(file)

          const status = yield* lsp.status()
          expect(status.filter((entry) => entry.id === "fake")).toHaveLength(1)
        }),
      ),
    {
      config: {
        lsp: {
          fake: { command: [process.execPath, fakeServerPath], extensions: [".repro"] },
        },
      },
    },
  )

  it.instance(
    "a cancelled and a failed call leave the one hub client reusable (no leak or duplicate)",
    () =>
      LSP.Service.use((lsp) =>
        Effect.gen(function* () {
          const dir = (yield* TestInstance).directory
          const file = path.join(dir, "cancel.repro")
          yield* Effect.promise(() => Bun.write(file, "sample\n"))
          yield* lsp.touchFile(file)

          yield* lsp
            .hover({ file, line: 0, character: 0 })
            .pipe(Effect.timeout("10 millis"), Effect.orElseSucceed(() => []))
          yield* lsp.hover({ file, line: 0, character: 0 }).pipe(Effect.orElseSucceed(() => []))

          const status = yield* lsp.status()
          expect(status.filter((entry) => entry.id === "fake")).toHaveLength(1)
          // A subsequent call still succeeds against the same client.
          yield* lsp.touchFile(file)
        }),
      ),
    {
      config: {
        lsp: {
          fake: { command: [process.execPath, fakeServerPath], extensions: [".repro"] },
        },
      },
    },
  )
})

describe("LSP.Diagnostic", () => {
  test("pretty() formats error diagnostic", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 9, character: 4 }, end: { line: 9, character: 10 } },
      message: "Type 'string' is not assignable to type 'number'",
      severity: 1,
    } as any)
    expect(result).toBe("ERROR [10:5] Type 'string' is not assignable to type 'number'")
  })

  test("pretty() formats warning diagnostic", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 5 } },
      message: "Unused variable",
      severity: 2,
    } as any)
    expect(result).toBe("WARN [1:1] Unused variable")
  })

  test("pretty() defaults to ERROR when no severity", () => {
    const result = LSP.Diagnostic.pretty({
      range: { start: { line: 0, character: 0 }, end: { line: 0, character: 1 } },
      message: "Something wrong",
    } as any)
    expect(result).toBe("ERROR [1:1] Something wrong")
  })
})
