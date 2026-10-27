import { describe, expect, test } from "bun:test"
import path from "node:path"
import { ToolRoute } from "../../src/tool/route"

const src = path.join(import.meta.dir, "../../src")

describe("tool routing — one decision point", () => {
  test("MCP and LSP implementations run on the hub", () => {
    for (const source of ["mcp", "lsp"] as const) {
      const route = ToolRoute.routeTool({ id: "arbitrary-id", source })
      expect(route).toEqual({ id: "arbitrary-id", source, host: "hub" })
      expect(ToolRoute.runsOn(route, "worker")).toBe(false)
      expect(ToolRoute.runsOn(route, "hub")).toBe(true)
    }
  })

  test("built-in, file and worker-hosted plugin tools run in the worker", () => {
    for (const source of ["builtin", "file", "plugin"] as const) {
      const route = ToolRoute.routeTool({ id: "arbitrary-id", source })
      expect(route.host).toBe("worker")
      expect(ToolRoute.runsOn(route, "worker")).toBe(true)
      // The hub is a superset and still runs built-ins in-process.
      expect(ToolRoute.runsOn(route, "hub")).toBe(true)
    }
  })

  test("the process role alone selects the host, and an unset role is the hub", () => {
    expect(ToolRoute.hostForRole("session")).toBe("worker")
    expect(ToolRoute.hostForRole("hub")).toBe("hub")
    expect(ToolRoute.hostForRole(undefined)).toBe("hub")
    expect(ToolRoute.hostForRole("writer")).toBe("hub")
  })

  test("only the LSP registry built-in is hub-sourced", () => {
    expect(ToolRoute.sourceForRegistryTool("lsp")).toBe("lsp")
    expect(ToolRoute.sourceForRegistryTool("bash")).toBe("builtin")
    expect(ToolRoute.sourceForRegistryTool("my_arbitrary_mcp_tool")).toBe("builtin")
    // The LSP tool is therefore unavailable in a worker, matching v1 parity.
    expect(ToolRoute.runsOn(ToolRoute.routeTool({ id: "lsp", source: "lsp" }), "worker")).toBe(false)
  })

  test("import boundary: only route.ts reads the process role for tool routing", async () => {
    const files: string[] = []
    for await (const file of new Bun.Glob("tool/*.ts").scan({ cwd: src, absolute: true, dot: true })) files.push(file)
    const readers = await Promise.all(
      files.map(async (file) => ({
        file: path.basename(file),
        text: await Bun.file(file).text(),
      })),
    )
    const roleReaders = readers
      .filter(({ text }) => /from ["']@\/effect\/process-role["']/.test(text))
      .map(({ file }) => file)
    expect(roleReaders).toEqual(["route.ts"])
  })

  test("import boundary: session tools route through the shared module", async () => {
    const text = await Bun.file(path.join(src, "session/tools.ts")).text()
    expect(text).toContain('from "@/tool/route"')
    expect(text).toContain("ToolRoute.hostForRole()")
    // No second local host predicate: the decision is not duplicated here.
    expect(text).not.toContain("processRole(")
  })
})

describe("tool routing — worker/hub parity and remote stubs", () => {
  test("MCP and LSP implementations stay hub-hosted", () => {
    for (const source of ["mcp", "lsp"] as const) {
      const route = ToolRoute.routeTool({ id: "arbitrary-id", source })
      expect(route.host).toBe("hub")
      expect(ToolRoute.runsOn(route, "hub")).toBe(true)
      expect(ToolRoute.runsOn(route, "worker")).toBe(false)
    }
  })

  test("remote plugin tools are forwarding stubs that run in the worker", () => {
    const route = ToolRoute.routeTool({ id: "remote_compress", source: "remote-plugin" })
    expect(route.host).toBe("worker")
    expect(ToolRoute.runsOn(route, "worker")).toBe(true)
  })

  test("every declared source maps to exactly one host", () => {
    const sources = ["builtin", "file", "plugin", "remote-plugin", "mcp", "lsp"] as const
    const hosts = Object.fromEntries(sources.map((source) => [source, ToolRoute.hostForSource(source)]))
    expect(hosts).toEqual({
      builtin: "worker",
      file: "worker",
      plugin: "worker",
      "remote-plugin": "worker",
      mcp: "hub",
      lsp: "hub",
    })
  })
})

describe("tool routing — recursive single-point guard", () => {
  test("no tool/** or session/** source defines a second routing predicate", async () => {
    const files: string[] = []
    for await (const file of new Bun.Glob("tool/**/*.ts").scan({ cwd: src, absolute: true, dot: true })) files.push(file)
    for await (const file of new Bun.Glob("session/**/*.ts").scan({ cwd: src, absolute: true, dot: true })) files.push(file)
    const sources = await Promise.all(
      files.map(async (file) => ({
        path: path.relative(src, file),
        text: await Bun.file(file).text(),
      })),
    )

    expect(ToolRoute.findSecondRoutingPredicates(sources)).toEqual([])
  })

  test("the guard sees a nested local host predicate, not only a leading keyword", () => {
    const nested = `
      import { processRole } from "@/effect/process-role"
      export function pickHost(source: string) {
        return processRole() === "session" && source === "mcp" ? "worker" : "hub"
      }
    `
    expect(ToolRoute.findSecondRoutingPredicates([{ path: "session/nested.ts", text: nested }])).toEqual([
      "session/nested.ts",
    ])
  })

  test("route.ts itself is the one allowed reader", () => {
    const allowed = "tool/route.ts"
    const text = `import { processRole } from "@/effect/process-role"\nconst role = processRole()\n`
    expect(ToolRoute.findSecondRoutingPredicates([{ path: allowed, text }])).toEqual([])
  })
})
