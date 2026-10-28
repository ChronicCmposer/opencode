// The broker is deleted, not disabled (D20).
//
// The tool routing point is the only local/hub decision. A broker file, client,
// port, layer node, build worker entry, source import, or non-.ts asset cannot
// come back without turning this gate red.
//
// The real roots are scanned (source, build scripts, package assets); the
// checked-in `fixtures/broker-regression` tree is the negative fixture that
// proves each rule still fires on a synthetic resurrection.

import { describe, expect, test } from "bun:test"
import path from "node:path"

const root = path.join(import.meta.dir, "../../..")
const src = path.join(root, "src")
const script = path.join(root, "script")
const fixture = path.join(import.meta.dir, "fixtures/broker-regression")

const BROKER_IMPORT = /from\s+["'](?:@\/session\/broker(?:\/[^"']*)?|\.\.?\/broker(?:\/[^"']*)?)["']/
const BROKER_SYMBOL =
  /\b(?:createBroker|serveBrokerPort|BrokerBackend|BrokerOptions|BrokerClientHandle|BrokerClientService|BrokerClientNode|BROKER_CLIENT_KEY|createWorkerBrokerClient)\b/
const BROKER_PATH = /\bsession\/broker\b|\bbroker-worker\b|\bbroker-client\b|\bworker\/broker\b/

const RULES = [BROKER_IMPORT, BROKER_SYMBOL, BROKER_PATH] as const

async function filesUnder(dir: string): Promise<string[]> {
  const files: string[] = []
  for await (const file of new Bun.Glob("**/*").scan({ cwd: dir, absolute: true, dot: true })) files.push(file)
  return files
}

const relative = (base: string, file: string) => path.relative(base, file).split(path.sep).join("/")

/** The guard: any scanned text that reintroduces a broker is a violation. */
async function scanBrokerRegressions(files: readonly string[], base: string): Promise<string[]> {
  const violations: string[] = []
  for (const file of files) {
    const text = await Bun.file(file).text()
    if (RULES.some((rule) => rule.test(text))) violations.push(relative(base, file))
  }
  return violations
}

/** Every text-bearing package file: source, build scripts, and package metadata. */
async function packageFiles(): Promise<string[]> {
  return [...(await filesUnder(src)), ...(await filesUnder(script)), path.join(root, "package.json")]
}

describe("broker deletion guard", () => {
  test("the broker source and worker client are gone", async () => {
    expect(await Bun.file(path.join(src, "session/broker/broker.ts")).exists()).toBe(false)
    expect(await Bun.file(path.join(src, "session/broker/broker-worker.ts")).exists()).toBe(false)
    expect(await Bun.file(path.join(src, "session/worker/broker-client.ts")).exists()).toBe(false)
  })

  test("no source, script, or package asset references the broker", async () => {
    expect(await scanBrokerRegressions(await packageFiles(), root)).toEqual([])
  })

  test("script and non-.ts package assets are scanned", async () => {
    const files = await packageFiles()
    expect(files.some((file) => file.endsWith("/script/build.ts"))).toBe(true)
    expect(files.some((file) => file.endsWith("/package.json"))).toBe(true)
    expect(files.some((file) => /\.(sql|txt|md)$/.test(file))).toBe(true)
  })

  test("the packaged worker entry set has no broker entry", async () => {
    const build = await Bun.file(path.join(script, "build.ts")).text()
    const entries = [...build.matchAll(/"([^"]*worker[^"]*\.ts)"/g)].map((match) => match[1]!)
    expect(entries).toContain("./src/session/worker/session-worker.ts")
    expect(entries.filter((entry) => /broker/.test(entry))).toEqual([])
  })

  test("a synthetic broker regression turns the guard red", async () => {
    const violations = await scanBrokerRegressions(await filesUnder(fixture), fixture)
    expect(violations.sort()).toEqual([
      "script/build.ts",
      "src/session/broker/broker.ts",
      "src/session/prompt/regression.txt",
      "src/session/worker/broker-client.ts",
    ])
  })
})
