// Build-source gate: every isolate entry spawned in `src/**` must be bundled by
// `script/build.ts`, and each build-time define it injects must be the one its
// client consumes.
//
// This is the source-level companion to the packaged-binary smoke gate
// (`worker-binary.test.ts`): that tier proves a single worker boots from the
// compiled artifact, while this one proves no worker entry was forgotten. A
// forgotten entry is invisible to in-process tests (they load workers from
// source) and only explodes in the packaged binary as
// `ModuleNotFound resolving "/$bunfs/root/<worker>.ts"` — the writer bug this
// gate exists for.

import { describe, expect, test } from "bun:test"
import path from "node:path"

const PACKAGE_DIR = path.resolve(import.meta.dir, "../..")

interface WorkerEntry {
  readonly name: string
  /** Source path listed in `entrypoints` so Bun bundles it into the binary. */
  readonly source?: string
  /** Flat bunfs shim embedded through `files` so a `new Worker` can resolve it. */
  readonly shim?: string
  /** Build-time define injected for this worker. */
  readonly define: string
  /** Client source that must read the define (`@opentui/core` is vendored). */
  readonly consumer: string
}

const ENTRIES: readonly WorkerEntry[] = [
  { name: "tui", source: "./src/cli/tui/worker.ts", define: "OPENCODE_WORKER_PATH", consumer: "src/cli/cmd/tui.ts" },
  {
    name: "session-worker",
    source: "./src/session/worker/session-worker.ts",
    shim: "session-worker.js",
    define: "OPENCODE_SESSION_WORKER_PATH",
    consumer: "src/session/worker/hub.ts",
  },
  {
    name: "writer-worker",
    source: "./src/session/writer/worker.ts",
    shim: "writer-worker.js",
    define: "OPENCODE_WRITER_WORKER_PATH",
    consumer: "src/session/writer/client.ts",
  },
  {
    name: "broker-worker",
    source: "./src/session/broker/broker-worker.ts",
    shim: "broker-worker.js",
    define: "OPENCODE_BROKER_WORKER_PATH",
    consumer: "src/session/broker/broker.ts",
  },
  {
    name: "tree-sitter",
    shim: "opentui-tree-sitter-worker.js",
    define: "OTUI_TREE_SITTER_WORKER_PATH",
    consumer: "@opentui/core",
  },
]

const read = (relative: string): Promise<string> => Bun.file(path.join(PACKAGE_DIR, relative)).text()

describe("script/build.ts bundles every worker entry", () => {
  test("each source entry is listed in `entrypoints`", async () => {
    const build = await read("script/build.ts")
    const missing = ENTRIES.filter((entry) => entry.source !== undefined && !build.includes(`"${entry.source}"`)).map(
      (entry) => entry.name,
    )
    expect(missing).toEqual([])
  })

  test("each flat shim is embedded in `files`", async () => {
    const build = await read("script/build.ts")
    const missing = ENTRIES.filter((entry) => entry.shim !== undefined && !build.includes(entry.shim)).map(
      (entry) => entry.name,
    )
    expect(missing).toEqual([])
  })

  test("each build-time define is injected", async () => {
    const build = await read("script/build.ts")
    const missing = ENTRIES.filter((entry) => !build.includes(`${entry.define}:`)).map((entry) => entry.name)
    expect(missing).toEqual([])
  })

  test("each define is consumed by its client", async () => {
    const missing: string[] = []
    for (const entry of ENTRIES) {
      if (entry.consumer === "@opentui/core") continue
      if (!(await read(entry.consumer)).includes(entry.define)) missing.push(entry.name)
    }
    expect(missing).toEqual([])
  })
})
