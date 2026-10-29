// Step 6 (S5): `@opencode/SessionPrompt` is declared once, in
// `prompt-service.ts`, and every consumer resolves that shared key.
//
// A second `Context.Service(...)("@opencode/SessionPrompt")` call is a different
// runtime key even though the string matches, so the hub resolver and the
// production/reduced graphs would silently stop seeing each other. This pins the
// single declaration, the back-compat re-export identity, and resolution through
// the shared service.

import { describe, expect, test } from "bun:test"
import { readFileSync } from "node:fs"
import path from "node:path"
import { Effect } from "effect"
import { appServiceNodes } from "@/effect/app-graph"
import { SessionPrompt } from "@/session/prompt"
import { SessionPromptService } from "@/session/prompt-service"
import { REQUIRED_SESSION_NODE_KEYS } from "@/session/worker/node-table"
import { SESSION_NODE_REGISTRY } from "@/session/worker/layer"

const SRC = path.join(import.meta.dir, "../../src")
const PROMPT_SERVICE = path.join(SRC, "session/prompt-service.ts")
const KEY = "@opencode/SessionPrompt"
const DECLARATION = /Context\.Service[^\n]*\(\)?\s*\(\s*"@opencode\/SessionPrompt"\s*\)/g

async function sourceFiles(): Promise<string[]> {
  const files: string[] = []
  for await (const file of new Bun.Glob("**/*.ts").scan({ cwd: SRC, absolute: true, dot: true })) files.push(file)
  return files
}

async function declarationsByFile(): Promise<string[]> {
  const files = await sourceFiles()
  return files.flatMap((file) => {
    const matches = readFileSync(file, "utf8").match(DECLARATION)
    if (matches === null) return []
    return matches.map(() => path.relative(path.join(SRC, ".."), file))
  })
}

describe("SessionPrompt service identity", () => {
  test("exactly one Context.Service declaration of the key exists in src", async () => {
    expect(await declarationsByFile()).toEqual(["src/session/prompt-service.ts"])
  })

  test("prompt-service.ts has no runtime import of prompt.ts or a layer", () => {
    const source = readFileSync(PROMPT_SERVICE, "utf8")
    expect(source).not.toMatch(/from\s+"\.\/prompt"/)
    expect(source).not.toMatch(/\bLayer\./)
    expect(source).not.toMatch(/\bLayerNode\b/)
  })

  test("the back-compat SessionPrompt.Service is the shared declaration", () => {
    expect(SessionPrompt.Service).toBe(SessionPromptService.Service)
    expect(SessionPrompt.Service.key).toBe(KEY)
    expect(SessionPromptService.Service.key).toBe(KEY)
  })

  test("the production graph carries the shared service key", () => {
    const node = appServiceNodes().find((candidate) => candidate.name === KEY)
    expect(node?.service).toBe(SessionPromptService.Service)
  })

  test("the reduced worker graph carries the shared service key", () => {
    expect(REQUIRED_SESSION_NODE_KEYS).toContain(KEY)
    expect(SESSION_NODE_REGISTRY.get(KEY)?.service).toBe(SessionPromptService.Service)
  })

  test("the shared Service resolves a provided implementation", async () => {
    const implementation: SessionPromptService.Interface = {
      cancel: () => Effect.succeed(undefined),
      prompt: () => Effect.die("unused in identity test"),
      loop: () => Effect.die("unused in identity test"),
      shell: () => Effect.die("unused in identity test"),
      command: () => Effect.die("unused in identity test"),
      resolvePromptParts: () => Effect.succeed([]),
    }

    const resolved = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* SessionPromptService.Service
      }).pipe(Effect.provideService(SessionPromptService.Service, implementation)),
    )

    expect(resolved).toBe(implementation)
  })
})
