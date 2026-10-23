// Structural guard: the session worker's STATIC (import-time) module graph.
//
// WHY THIS TEST EXISTS
// --------------------
// The session worker entry (`src/session/worker/session-worker.ts`) is bundled
// with `splitting: true` (`script/build.ts`). Its isolate must stay lean: it may
// NOT eagerly evaluate the heavy modules that pull in the whole server / app
// runtime. The intended design keeps those behind a *deferred* boundary, e.g.
//
//     Layer.unwrap(Effect.promise(() => import("./loader")))   // dynamic-import
//
// `Bun.build({ metafile: true })` exposes the module graph: every input lists
// its `imports[]` with a `kind`. `"import-statement"` is a STATIC edge (the
// target evaluates before the entry body runs); `"dynamic-import"` is a lazy
// edge (the target only evaluates when the `import()` expression executes).
//
// INVARIANT UNDER TEST
// --------------------
//   The forbidden modules must NOT be reachable from the worker entry by
//   following `"import-statement"` edges transitively.
//
// Dynamic-only reachability is *allowed and expected*: it is exactly the point
// of the deferred boundary, so a forbidden module may still appear in
// `metafile.inputs` (because some `import()` references it) — we only assert it
// is absent from the *static* closure. We deliberately follow only
// `"import-statement"` edges: for a `target: "bun"` ESM build the package's own
// `src/**` modules are wired exclusively through `import-statement` /
// `dynamic-import` (there are zero `require-call` edges originating in `src/**`;
// the `require-call` entries belong to CommonJS in `node_modules`).
//
// DEPENDENCY: this guard was RED before the ws1 (plugin light split) and ws2
// (worker-layer replacement) workstreams landed. While RED it reported the exact
// leaking static edges (`session-worker.ts -> session/prompt.ts ->
// src/plugin/index.ts -> {plugin/effect-layer.ts, control-plane/adapters/**}`), which
// is the evidence that drove those splits. It is expected to stay GREEN now that
// `src/plugin/index.ts` reaches its heavy layer through a deferred `import()`.

import { describe, expect, test } from "bun:test"
import path from "node:path"

/** `packages/opencode` — the bundler root for the worker entry. */
const PACKAGE_ROOT = path.resolve(import.meta.dir, "../../..")
const WORKER_ENTRY = path.join(PACKAGE_ROOT, "src/session/worker/session-worker.ts")
/** Suffix that uniquely identifies the worker entry among metafile inputs. */
const WORKER_ENTRY_SUFFIX = "src/session/worker/session-worker.ts"

const toPosix = (modulePath: string): string => modulePath.replaceAll("\\", "/")

/**
 * Matches a module at an exact repo-relative path. Metafile keys are relative
 * to the build's working directory, so the same module may be keyed as
 * `src/foo.ts`, `./src/foo.ts`, or `../../abs/src/foo.ts`; match the suffix at a
 * path boundary so the guard is independent of cwd.
 */
const atModule = (suffix: string) => (modulePath: string) => {
  const p = toPosix(modulePath)
  return p === suffix || p.endsWith(`/${suffix}`)
}
/** Matches every module inside a repo-relative directory (any cwd prefix). */
const underDirectory = (dir: string) => (modulePath: string) => {
  const p = toPosix(modulePath)
  return p === dir || p.startsWith(`${dir}/`) || p.includes(`/${dir}/`)
}

interface ForbiddenModule {
  /** Human label used in the failure message. */
  readonly label: string
  readonly matches: (modulePath: string) => boolean
  /** Why this module must not be eagerly evaluated in the worker isolate. */
  readonly reason: string
}

/**
 * The heavy modules the worker entry must not reach at import time. These are
 * the modules that transitively pull the server / app runtime; the deferred
 * `import()` boundary exists precisely to keep them off the static path.
 */
const FORBIDDEN_STATIC_MODULES: readonly ForbiddenModule[] = [
  {
    label: "src/plugin/effect-layer.ts",
    matches: atModule("src/plugin/effect-layer.ts"),
    reason: "the plugin effect-layer evaluates the full plugin/server surface",
  },
  {
    label: "src/server/server.ts",
    matches: atModule("src/server/server.ts"),
    reason: "the HTTP server is not part of the worker isolate",
  },
  {
    label: "src/effect/app-runtime.ts",
    matches: atModule("src/effect/app-runtime.ts"),
    reason: "the app runtime builds the full application layer",
  },
  {
    label: "src/control-plane/adapters/**",
    matches: underDirectory("src/control-plane/adapters"),
    reason: "control-plane adapters are hub-side only",
  },
]

/**
 * Build the worker entry exactly as production does (`script/build.ts`): bun
 * target, ESM, code splitting, and an in-memory metafile describing the graph.
 * A failed build must fail loud with the bundler's own diagnostics.
 */
async function buildWorkerMetafile(): Promise<Bun.BuildMetafile> {
  const result = await Bun.build({
    entrypoints: [WORKER_ENTRY],
    target: "bun",
    format: "esm",
    splitting: true,
    conditions: ["bun", "node"],
    tsconfig: path.join(PACKAGE_ROOT, "tsconfig.json"),
    external: ["node-gyp"],
    metafile: true,
  })
  if (!result.success || result.metafile === undefined) {
    const diagnostics = result.logs.map((log) => String(log)).join("\n")
    throw new Error(`session worker build failed:\n${diagnostics}`)
  }
  return result.metafile
}

/** Locate the worker entry among the metafile inputs, or fail loud. */
function findWorkerEntryKey(inputs: Bun.BuildMetafile["inputs"]): string {
  const entry = Object.keys(inputs).find((key) => toPosix(key).endsWith(WORKER_ENTRY_SUFFIX))
  if (entry === undefined) {
    throw new Error(`metafile has no input ending in "${WORKER_ENTRY_SUFFIX}"`)
  }
  return entry
}

interface StaticReachability {
  /** Every module reachable from the entry through `"import-statement"` edges. */
  readonly reachable: ReadonlySet<string>
  /** BFS parent of each reachable module; the entry maps to `undefined`. */
  readonly parentOf: ReadonlyMap<string, string | undefined>
}

/**
 * Walk the STATIC graph: BFS from the worker entry following only
 * `"import-statement"` edges whose target is a known input (external specifiers
 * such as `node:worker_threads` are not in the graph and are skipped).
 */
function collectStaticReachability(inputs: Bun.BuildMetafile["inputs"], entryKey: string): StaticReachability {
  const reachable = new Set<string>([entryKey])
  const parentOf = new Map<string, string | undefined>([[entryKey, undefined]])
  const queue: string[] = [entryKey]
  while (queue.length > 0) {
    const current = queue.shift()!
    for (const imported of inputs[current].imports) {
      if (imported.kind !== "import-statement") continue
      // With `splitting`, a static edge resolves to an input path directly; fall
      // back to `entryPoint` in case the bundler routes it through a chunk.
      const target = imported.path in inputs ? imported.path : imported.entryPoint
      if (target === undefined || !(target in inputs)) continue
      if (reachable.has(target)) continue
      reachable.add(target)
      parentOf.set(target, current)
      queue.push(target)
    }
  }
  return { reachable, parentOf }
}

/** Reconstruct the static import chain that reaches `target` (entry first). */
function describeStaticPath(target: string, parentOf: ReadonlyMap<string, string | undefined>): string[] {
  const chain: string[] = []
  let cursor: string | undefined = target
  while (cursor !== undefined) {
    chain.unshift(toPosix(cursor))
    cursor = parentOf.get(cursor)
  }
  return chain
}

/** Every forbidden module that is statically reachable, with its import chain. */
function findStaticLeaks(
  reachable: ReadonlySet<string>,
  parentOf: ReadonlyMap<string, string | undefined>,
): string[] {
  const leaks: string[] = []
  for (const forbidden of FORBIDDEN_STATIC_MODULES) {
    for (const modulePath of reachable) {
      if (!forbidden.matches(modulePath)) continue
      const chain = describeStaticPath(modulePath, parentOf)
      leaks.push(`${forbidden.label} (${forbidden.reason}) is statically reachable:\n    ${chain.join("\n -> ")}`)
    }
  }
  return leaks
}

describe("session worker static module graph", () => {
  test("forbidden heavy modules are not statically reachable from the worker entry", async () => {
    const metafile = await buildWorkerMetafile()
    const entryKey = findWorkerEntryKey(metafile.inputs)
    const { reachable, parentOf } = collectStaticReachability(metafile.inputs, entryKey)
    // A trivially-small build would make the structural guard vacuously pass.
    expect(reachable.size).toBeGreaterThan(200)
    const leaks = findStaticLeaks(reachable, parentOf)
    expect(leaks).toEqual([])
  }, 30_000)

  // Positive control: proves the BFS resolves real static edges (and that key
  // matching works against the actual metafile key format). Without this, a BFS
  // that silently matched nothing would pass the leak test vacuously.
  test("static traversal reaches known eagerly-imported modules", async () => {
    const metafile = await buildWorkerMetafile()
    const entryKey = findWorkerEntryKey(metafile.inputs)
    const { reachable } = collectStaticReachability(metafile.inputs, entryKey)
    const eagerlyImported = ["src/session/prompt.ts", "src/session/worker/layer.ts", "src/session/worker/execute.ts"]
    for (const module of eagerlyImported) {
      const key = Object.keys(metafile.inputs).find((candidate) => toPosix(candidate).endsWith(module))
      expect(key).toBeDefined()
      expect(reachable.has(key!)).toBe(true)
    }
  }, 30_000)
})
