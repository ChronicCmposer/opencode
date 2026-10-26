// W4.3 / D20 — the broker is deleted, not disabled.
//
// The tool routing point is the only local/hub decision. A broker file, client,
// port, layer node, or source import cannot come back without turning this gate
// red.

import { describe, expect, test } from "bun:test"
import path from "node:path"

const root = path.join(import.meta.dir, "../../..")
const src = path.join(root, "src")

const BROKER_IMPORT = /from\s+["'](?:@\/session\/broker(?:\/[^"']*)?|\.\.?\/broker(?:\/[^"']*)?)["']/
const BROKER_SYMBOL =
  /\b(?:createBroker|serveBrokerPort|BrokerBackend|BrokerOptions|BrokerClientHandle|BrokerClientService|BrokerClientNode|BROKER_CLIENT_KEY|createWorkerBrokerClient)\b/

async function sourceFiles(): Promise<string[]> {
  const files: string[] = []
  for await (const file of new Bun.Glob("**/*.ts").scan({ cwd: src, absolute: true, dot: true })) files.push(file)
  return files
}

const relative = (file: string) => path.relative(src, file).split(path.sep).join("/")

describe("W4.3 — broker deletion guard", () => {
  test("the broker source and worker client are gone", async () => {
    expect(await Bun.file(path.join(src, "session/broker/broker.ts")).exists()).toBe(false)
    expect(await Bun.file(path.join(src, "session/broker/broker-worker.ts")).exists()).toBe(false)
    expect(await Bun.file(path.join(src, "session/worker/broker-client.ts")).exists()).toBe(false)
  })

  test("no source file imports the broker", async () => {
    const importers: string[] = []
    for (const file of await sourceFiles()) {
      if (BROKER_IMPORT.test(await Bun.file(file).text())) importers.push(relative(file))
    }
    expect(importers).toEqual([])
  })

  test("no broker client, port, or layer node symbol remains", async () => {
    const symbols: string[] = []
    for (const file of await sourceFiles()) {
      if (BROKER_SYMBOL.test(await Bun.file(file).text())) symbols.push(relative(file))
    }
    expect(symbols).toEqual([])
  })
})
