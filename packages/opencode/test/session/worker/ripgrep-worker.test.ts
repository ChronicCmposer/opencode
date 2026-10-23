// Fix 2 — the worker's `Ripgrep.Service` must be REAL inside a session worker.
//
// Before Fix 2 the reduced session graph replaced `v2/Ripgrep` with a fail-loud
// `ripgrepBoundaryLayer`, so any worker-side search died with
// `SessionLayerBoundaryError`. These tests build the production reduced graph
// (`RUNNABLE_SESSION_NODES` + the worker `boundaryReplacements`) and call
// `grep` / `glob` / `find` on it, asserting real results and no boundary defect.
//
// The search runs a real `rg`: the test snapshot pins `ripgrepPath` at
// `/usr/bin/rg` and `snapshotRipgrepBinaryLayer` falls back to `"rg"` on PATH.

import { describe, expect, test } from "bun:test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { Effect, Layer } from "effect"
import { Ripgrep } from "@opencode-ai/core/ripgrep"
import { SessionLayerBoundaryError } from "@/session/worker/layer"
import { buildWorkerLayer, runWorker, runWithLayer } from "../../lib/worker-ripgrep-harness"

/** A small fixture tree the searches run against. */
function fixture(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "oc-ripgrep-worker-"))
  fs.writeFileSync(path.join(dir, "alpha.txt"), "hello needle world\n")
  fs.writeFileSync(path.join(dir, "beta.md"), "# beta\nneedle in markdown\n")
  fs.mkdirSync(path.join(dir, "nested"))
  fs.writeFileSync(path.join(dir, "nested", "gamma.ts"), "export const needle = 1\n")
  return dir
}

const withFixture = <A>(use: (dir: string) => Promise<A>): Promise<A> => {
  const dir = fixture()
  return use(dir).finally(() => fs.rmSync(dir, { recursive: true, force: true }))
}

describe("worker Ripgrep (Fix 2)", () => {
  test("grep executes in-worker and returns real matches", () =>
    withFixture(async (dir) => {
      const matches = await runWorker(
        Effect.gen(function* () {
          const ripgrep = yield* Ripgrep.Service
          return yield* ripgrep.grep({ cwd: dir, pattern: "needle", limit: 100 })
        }),
      )

      expect(matches.length).toBeGreaterThanOrEqual(3)
      expect(matches.map((match) => String(match.entry.path)).sort()).toEqual([
        "alpha.txt",
        "beta.md",
        "nested/gamma.ts",
      ])
      expect(matches.some((match) => match.text.includes("needle"))).toBe(true)
    }))

  test("glob executes in-worker and lists matching files", () =>
    withFixture(async (dir) => {
      const files = await runWorker(
        Effect.gen(function* () {
          const ripgrep = yield* Ripgrep.Service
          return yield* ripgrep.glob({ cwd: dir, pattern: "*.txt", limit: 100 })
        }),
      )

      expect(files.map((file) => String(file.path))).toEqual(["alpha.txt"])
    }))

  test("find executes in-worker with an AbortSignal and lists matching files", () =>
    withFixture(async (dir) => {
      const files = await runWorker(
        Effect.gen(function* () {
          const ripgrep = yield* Ripgrep.Service
          return yield* ripgrep.find({
            cwd: dir,
            pattern: "*.md",
            limit: 100,
            signal: new AbortController().signal,
          })
        }),
      )

      expect(files.map((file) => String(file.path))).toEqual(["beta.md"])
    }))

  test("a search that matches nothing still runs in-worker (no boundary defect)", () =>
    withFixture(async (dir) => {
      const matches = await runWorker(
        Effect.gen(function* () {
          const ripgrep = yield* Ripgrep.Service
          return yield* ripgrep.grep({ cwd: dir, pattern: "zzz-no-such-token-zzz", limit: 100 })
        }),
      )

      expect(matches).toEqual([])
    }))

  // Guard the guard: the harness must FAIL when `Ripgrep` is the old fail-loud
  // boundary, otherwise a green run would prove nothing. Reinstall exactly that
  // boundary via a replacement and assert the descriptive regression error.
  test("the harness reports a fail-loud Ripgrep boundary as a regression", async () => {
    const deny = (member: string) => () => Effect.die(new SessionLayerBoundaryError("Ripgrep", member))
    const boundary = { find: deny("find"), glob: deny("glob"), grep: deny("grep") } as unknown as Ripgrep.Interface
    const boundaryLayer = buildWorkerLayer({
      replacements: [[Ripgrep.node, Layer.succeed(Ripgrep.Service, boundary)]],
    })

    await expect(
      runWithLayer(
        boundaryLayer,
        Effect.gen(function* () {
          const ripgrep = yield* Ripgrep.Service
          return yield* ripgrep.grep({ cwd: os.tmpdir(), pattern: "x", limit: 1 })
        }),
      ),
    ).rejects.toThrow(/worker boundary regression/)
  })
})
