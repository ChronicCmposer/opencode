import { describe, expect, test } from "bun:test"
import path from "path"

// The light entry. Importing `@/plugin` must not statically pull the server graph.
const pluginEntry = path.resolve(import.meta.dir, "../../src/plugin/index.ts")

// Modules that importing `@/plugin` must never reach through *static* imports.
const HEAVY_MODULES = [
  "src/control-plane/adapters",
  "src/server/server",
  "src/server/auth",
  "src/effect/app-runtime",
  "src/session/session",
  "src/plugin/openai/codex",
  "src/plugin/github-copilot",
  "src/plugin/modal",
  "src/plugin/cloudflare",
  "src/plugin/azure",
  "src/plugin/digitalocean",
  "src/plugin/xai",
  "src/plugin/cerebras",
  "src/plugin/snowflake-cortex",
] as const

type InputNode = {
  readonly imports: ReadonlyArray<{ readonly path: string; readonly kind: string; readonly external?: boolean }>
}
type Metafile = { readonly inputs: Readonly<Record<string, InputNode>> }

// Bun.build reports every input relative to the build cwd, including files that are
// only reached through a dynamic import. To prove `@/plugin` is server-free we follow
// only static edges (not dynamic imports, not externals) from the entry.
function staticClosure(metafile: Metafile, entry: string): Set<string> {
  const byKey = new Map<string, InputNode>()
  for (const [key, node] of Object.entries(metafile.inputs)) {
    byKey.set(path.posix.normalize(key), node)
  }

  const entryKey = [...byKey.keys()].find((key) => entry.endsWith(key) || key === entry)
  if (!entryKey) throw new Error(`Entry ${entry} missing from metafile inputs`)

  const seen = new Set<string>()
  const stack = [entryKey]
  while (stack.length > 0) {
    const current = stack.pop()!
    if (seen.has(current)) continue
    seen.add(current)

    const node = byKey.get(current)
    if (!node) continue
    for (const edge of node.imports) {
      if (edge.external) continue
      if (edge.kind === "dynamic-import") continue
      stack.push(path.posix.normalize(edge.path))
    }
  }
  return seen
}

describe("plugin entry module graph", () => {
  test(
    "importing @/plugin does not statically pull the server graph",
    async () => {
      const result = await Bun.build({
        entrypoints: [pluginEntry],
        target: "bun",
        splitting: true,
        metafile: true,
      })
      if (!result.success) throw new Error(`Bun.build failed:\n${result.logs.map((log) => log.message).join("\n")}`)

      const closure = staticClosure(result.metafile! as Metafile, pluginEntry)
      const reached = HEAVY_MODULES.filter((needle) => [...closure].some((key) => key.includes(needle)))

      expect(reached).toEqual([])
    },
    60_000,
  )
})
