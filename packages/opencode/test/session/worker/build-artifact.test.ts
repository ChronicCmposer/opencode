// Packaged artifact gate for the worker build identity.
//
// The unit tests prove the resolver's modes. This test compiles real Bun
// binaries with an embedded `OPENCODE_BUILD_HASH` and exercises the production
// ready-frame check across two independently compiled artifacts: a matching pair
// passes, a mismatched pair fails with the typed `BuildMismatch` exit. It also
// pins that the runtime module carries no bundler call.

import fs from "node:fs"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { afterEach, describe, expect, test } from "bun:test"

const directories: string[] = []
const BUILD_HASH_MODULE_PATH = fileURLToPath(new URL("../../../src/session/worker/build-hash.ts", import.meta.url))

function workspace(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "opencode-build-artifact-"))
  directories.push(directory)
  return directory
}

function fixtureSource(): string {
  return `
import { readyFrame, resolveBuildHash, verifyReadyBuild } from ${JSON.stringify(BUILD_HASH_MODULE_PATH)}
const mode = process.argv[2]
if (mode === "frame") {
  console.log(JSON.stringify(readyFrame(1)))
} else if (mode === "resolve") {
  try {
    console.log(resolveBuildHash().hash)
  } catch (error) {
    console.log(error instanceof Error ? error.name : String(error))
    process.exit(3)
  }
} else {
  try {
    verifyReadyBuild(JSON.parse(process.argv[3]))
    console.log("ok")
  } catch {
    console.log("mismatch")
    process.exit(2)
  }
}
`
}

async function compile(directory: string, name: string, buildHash?: string): Promise<string> {
  const entrypoint = path.join(directory, `${name}.ts`)
  const outfile = path.join(directory, name)
  await Bun.write(entrypoint, fixtureSource())
  const result = await Bun.build({
    entrypoints: [entrypoint],
    compile: { outfile, autoloadBunfig: false, autoloadDotenv: false, autoloadTsconfig: true },
    define: {
      ...(buildHash === undefined ? {} : { OPENCODE_BUILD_HASH: JSON.stringify(buildHash) }),
      OPENCODE_PACKAGED: "true",
    },
    target: "bun",
  })
  expect(result.logs.map((log) => log.message)).toEqual([])
  expect(result.success).toBe(true)
  return outfile
}

async function run(
  binary: string,
  args: readonly string[],
): Promise<{ readonly exitCode: number; readonly out: string; readonly err: string }> {
  const child = Bun.spawn([binary, ...args], { stdout: "pipe", stderr: "pipe" })
  const [out, err] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()])
  const exitCode = await child.exited
  return { exitCode, out: out.trim(), err: err.trim() }
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true })
})

describe("packaged worker build identity", () => {
  test(
    "a compiled artifact reports its embedded hash as a ready frame",
    async () => {
      const directory = workspace()
      const worker = await compile(directory, "worker", "worker-hash")
      const frame = await run(worker, ["frame"])
      expect(frame.err).toBe("")
      expect(frame.exitCode).toBe(0)
      expect(JSON.parse(frame.out)).toEqual({ type: "ready", threadId: 1, buildHash: "worker-hash" })
    },
    120_000,
  )

  test(
    "the correct embedded hash passes and a wrong one fails",
    async () => {
      const directory = workspace()
      const worker = await compile(directory, "worker", "worker-hash")
      const matchingHub = await compile(directory, "hub-match", "worker-hash")
      const mismatchedHub = await compile(directory, "hub-mismatch", "other-hash")

      const frame = await run(worker, ["frame"])
      expect(frame.exitCode).toBe(0)

      const matching = await run(matchingHub, ["verify", frame.out])
      expect({ exitCode: matching.exitCode, out: matching.out }).toEqual({ exitCode: 0, out: "ok" })

      const mismatched = await run(mismatchedHub, ["verify", frame.out])
      expect({ exitCode: mismatched.exitCode, out: mismatched.out }).toEqual({ exitCode: 2, out: "mismatch" })
    },
    180_000,
  )

  test(
    "a packaged artifact without an embedded hash fails loud",
    async () => {
      const directory = workspace()
      const missing = await compile(directory, "missing-hash")
      const result = await run(missing, ["resolve"])
      expect({ exitCode: result.exitCode, out: result.out }).toEqual({
        exitCode: 3,
        out: "MissingBuildHashError",
      })
    },
    120_000,
  )

  test("the runtime build-hash module never calls the bundler", async () => {
    const source = await Bun.file(BUILD_HASH_MODULE_PATH).text()
    expect(source).not.toMatch(/Bun\.build\s*\(/)
    expect(source).not.toContain("hashWorkerBundle")
  })
})
