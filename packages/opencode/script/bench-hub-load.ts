// Hub main-thread load driver: boots `opencode serve` with session workers, streams `/global/event` like a UI,
// runs back-to-back turns against a mock provider, and samples the server's per-thread CPU (the busiest thread is
// the hub main thread). Compare decoders with OPENCODE_EGRESS_SCAN=js vs unset.
//
//   OPENCODE_EGRESS_SCAN=js PROFILE=0 bun run script/bench-hub-load.ts   (js decoder)
//   PROFILE=0 bun run script/bench-hub-load.ts                           (native when available)
//   PROF_DIR=/tmp/prof bun run script/bench-hub-load.ts                   (also writes a bun CPU profile)
//
// Env: SESSIONS (4), LOAD_SECONDS (60), IDLE_SECONDS (15), CHUNKS (2000 deltas per turn), PROFILE (1; 0 skips the
// profile and PROF_DIR), WORKERS=0 (run without session workers), OPENCODE_EGRESS_SCAN (passed to the server).
// Prints turns, SSE bytes, and "top thread CPU%" samples.
import { mkdirSync, mkdtempSync, realpathSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { startMockProvider } from "../test/lib/mock-provider"
import { testProviderConfig } from "../test/lib/test-provider"

const PROF_DIR = process.env["PROF_DIR"]
const SESSIONS = Number(process.env["SESSIONS"] ?? 4)
const LOAD_SECONDS = Number(process.env["LOAD_SECONDS"] ?? 60)
const IDLE_SECONDS = Number(process.env["IDLE_SECONDS"] ?? 15)
const CHUNKS = Number(process.env["CHUNKS"] ?? 2000)
const PROFILE = process.env["PROFILE"] !== "0"
if (PROFILE && PROF_DIR === undefined) throw new Error("PROF_DIR is required unless PROFILE=0")
const USER = "opencode"
const PASS = "load-pass"
const auth = { authorization: `Basic ${Buffer.from(`${USER}:${PASS}`).toString("base64")}` }
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

const home = realpathSync(mkdtempSync(path.join(tmpdir(), "oc-hub-load-")))
const worktree = path.join(home, "worktree")
mkdirSync(worktree, { recursive: true })
Bun.spawnSync(["git", "init", "--quiet"], { cwd: worktree })
const chunks = Array.from({ length: CHUNKS }, (_, i) => `tok${i} `)
await using provider = startMockProvider({ chunks, usage: { input: 1000, output: CHUNKS } })
const config = { ...testProviderConfig(provider.baseUrl), model: "test/test-model" }
const dataDir = path.join(home, "data")
const env: Record<string, string> = {
  PATH: process.env["PATH"] ?? "",
  TMPDIR: tmpdir(),
  HOME: home,
  XDG_CONFIG_HOME: path.join(home, ".config"),
  XDG_DATA_HOME: dataDir,
  XDG_STATE_HOME: path.join(home, ".local/state"),
  XDG_CACHE_HOME: path.join(home, ".cache"),
  OPENCODE_DB: path.join(dataDir, "opencode", "opencode.db"),
  OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
  OPENCODE_DISABLE_PROJECT_CONFIG: "1",
  OPENCODE_DISABLE_AUTOUPDATE: "1",
  OPENCODE_DISABLE_MODELS_FETCH: "1",
  OPENCODE_DISABLE_AUTOCOMPACT: "1",
  OPENCODE_EXPERIMENTAL_SESSION_WORKERS: process.env["WORKERS"] === "0" ? "false" : "true",
  OPENCODE_SERVER_PASSWORD: PASS,
  OPENCODE_SERVER_USERNAME: USER,
  ...(process.env["OPENCODE_EGRESS_SCAN"] === undefined ? {} : { OPENCODE_EGRESS_SCAN: process.env["OPENCODE_EGRESS_SCAN"] }),
}
const entry = path.join(import.meta.dir, "../src/index.ts")
const bunArgs = PROFILE ? ["--cpu-prof-md", `--cpu-prof-dir=${PROF_DIR}`, "--cpu-prof-interval=500"] : []
const server = Bun.spawn(
  [process.execPath, ...bunArgs, entry, "serve", "--hostname", "127.0.0.1", "--port", "0", "--log-level", "INFO"],
  { cwd: worktree, env, stdout: "pipe", stderr: "pipe" },
)
let out = ""
const drain = async (stream: ReadableStream<Uint8Array>, onText?: (text: string) => void) => {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) return
    onText?.(decoder.decode(value, { stream: true }))
  }
}
void drain(server.stdout, (text) => (out += text))
void drain(server.stderr)
let url: string | undefined
for (let i = 0; i < 600 && url === undefined; i++) {
  url = /listening on (http:\/\/[^\s]+)/.exec(out)?.[1]
  await sleep(100)
}
if (url === undefined) throw new Error("server never listened")
const q = `directory=${encodeURIComponent(worktree)}`

// A UI-like SSE subscriber.
const sse = new AbortController()
let sseBytes = 0
void fetch(`${url}/global/event`, { headers: auth, signal: sse.signal })
  .then(async (res) => {
    const reader = res.body!.getReader()
    while (true) {
      const { done, value } = await reader.read()
      if (done) return
      sseBytes += value.byteLength
    }
  })
  .catch(() => {})

const cpuOf = () => {
  const r = Bun.spawnSync(["ps", "-o", "%cpu=", "-M", "-p", String(server.pid)])
  return r.stdout.toString().trim()
}
// Per-thread CPU: ps -M lists threads; the busiest line is usually the JS main thread.
const sampleThreads = (label: string) => {
  const r = Bun.spawnSync(["ps", "-M", "-p", String(server.pid)])
  const lines = r.stdout.toString().trim().split("\n").slice(1)
  const cpus = lines
    .map((l) => Number(l.trim().split(/\s+/)[3]))
    .filter((n) => !Number.isNaN(n))
    .sort((a, b) => b - a)
  console.log(`${label}: top thread CPU% ${cpus.slice(0, 6).join(", ")}`)
}
void cpuOf

console.log(`scan=${process.env["OPENCODE_EGRESS_SCAN"] ?? "auto"} listening ${url}; idling ${IDLE_SECONDS}s`)
for (let i = 0; i < IDLE_SECONDS; i += 5) {
  await sleep(5000)
  sampleThreads(`idle t+${i + 5}s`)
}

const sessionIDs: string[] = []
for (let i = 0; i < SESSIONS; i++) {
  const res = await fetch(`${url}/session?${q}`, {
    method: "POST",
    headers: { ...auth, "content-type": "application/json" },
    body: JSON.stringify({ title: `load ${i}` }),
  })
  sessionIDs.push(((await res.json()) as { id: string }).id)
}
const deadline = Date.now() + LOAD_SECONDS * 1000
let turns = 0
const loop = async (id: string) => {
  while (Date.now() < deadline) {
    const res = await fetch(`${url}/session/${id}/message?${q}`, {
      method: "POST",
      headers: { ...auth, "content-type": "application/json" },
      body: JSON.stringify({ parts: [{ type: "text", text: `turn ${turns}` }] }),
    })
    await res.text()
    turns++
  }
}
const sampler = (async () => {
  while (Date.now() < deadline) {
    await sleep(5000)
    sampleThreads(`load turns=${turns}`)
  }
})()
await Promise.all([...sessionIDs.map(loop), sampler])
console.log(`turns=${turns} sseBytes=${sseBytes}`)
for (let i = 0; i < 2; i++) {
  await sleep(5000)
  sampleThreads(`after-load idle`)
}
sse.abort()
server.kill("SIGTERM")
await server.exited
console.log("done")
