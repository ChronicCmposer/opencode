// Plan task 3.8: fake MCP/LSP child-process fixture.
//
// Spawns the `mcp-lsp-descendants` fixture detached (its own process group) so
// it hosts a minimal MCP or LSP stdio server *and* builds a descendant tree.
// Asserts the tree cardinality independently (via /proc, not the fixture's own
// report), that every descendant shares the root's PGID, and that a single
// process-group kill reaps the whole tree. A negative case shows that killing
// only the root PID leaves descendants alive — the reason group-kill is the
// required teardown semantics in `src/mcp/index.ts`.
//
// POSIX-only: PGIDs do not exist on Windows, so those cases are skipped with a
// documented reason (`posixSupport().reason`).

import { describe, expect, test } from "bun:test"
import path from "node:path"
import {
  expectedDescendantCount,
  isAlive,
  killProcessGroup,
  listDescendants,
  posixSupport,
  processGroupId,
  spawnDetached,
  waitFor,
  waitForDeath,
  type DetachedChild,
} from "./process-tree"

const fixture = path.join(import.meta.dir, "../fixture/mcp-lsp-descendants.ts")
const support = posixSupport()

const FANOUT = 3
const DEPTH = 2
const EXPECTED = expectedDescendantCount(FANOUT, DEPTH)

function startTree(kind: "mcp" | "lsp", fanout: number, depth: number): DetachedChild {
  return spawnDetached([process.execPath, fixture], {
    stdin: "pipe",
    stdout: "pipe",
    env: {
      CHILD_TREE_KIND: kind,
      CHILD_TREE_FANOUT: String(fanout),
      CHILD_TREE_DEPTH: String(depth),
    },
  })
}

async function handshake(kind: "mcp" | "lsp", child: DetachedChild): Promise<Record<string, unknown>> {
  if (kind === "mcp") {
    child.write(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: { protocolVersion: "2024-11-05", capabilities: {}, clientInfo: { name: "test", version: "1" } },
      }) + "\n",
    )
    const line = await child.reader.readLine(5000)
    return JSON.parse(line) as Record<string, unknown>
  }

  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: { processId: process.pid, rootUri: null, capabilities: {} },
  })
  child.write(`Content-Length: ${Buffer.byteLength(body, "utf8")}\r\n\r\n${body}`)
  const header = await child.reader.readLine(5000)
  const match = /Content-Length:\s*(\d+)/i.exec(header)
  if (!match) throw new Error(`lsp fixture did not send a Content-Length header: ${header}`)
  await child.reader.readLine(5000) // blank separator
  const response = await child.reader.readExact(Number.parseInt(match[1]!, 10), 5000)
  return JSON.parse(response) as Record<string, unknown>
}

describe("fake MCP/LSP child-process fixture (3.8)", () => {
  for (const kind of ["mcp", "lsp"] as const) {
    test.skipIf(!support.supported)(
      `${kind}: hosts the protocol, builds a ${FANOUT}-ary tree of depth ${DEPTH}, and a PGID kill reaps it`,
      async () => {
        const child = startTree(kind, FANOUT, DEPTH)
        const pgid = child.pgid

        try {
          const response = await handshake(kind, child)
          const result = response.result as { capabilities?: unknown; serverInfo?: { name?: string } } | undefined
          expect(result?.capabilities).toBeDefined()
          expect(result?.serverInfo?.name).toBe("mcp-lsp-descendants")

          const descendants = await waitFor(
            () => {
              const pids = listDescendants(child.pid)
              return pids.length === EXPECTED ? pids : undefined
            },
            { message: `expected ${EXPECTED} descendants under pid ${child.pid}` },
          )
          expect(descendants).toHaveLength(EXPECTED)

          expect(processGroupId(child.pid)).toBe(pgid)
          for (const pid of descendants) {
            expect(isAlive(pid)).toBe(true)
            expect(processGroupId(pid)).toBe(pgid)
          }

          killProcessGroup(pgid, "SIGTERM")
          expect(await waitForDeath([child.pid, ...descendants], { timeoutMs: 10_000 })).toBe(true)
          expect(isAlive(child.pid)).toBe(false)
          for (const pid of descendants) expect(isAlive(pid)).toBe(false)
        } finally {
          killProcessGroup(pgid, "SIGKILL")
          await child.exited
        }
      },
      20_000,
    )
  }

  test.skipIf(!support.supported)(
    "killing only the root PID leaves descendants alive (why PGID kill is required)",
    async () => {
      const child = startTree("mcp", 2, 1)
      const pgid = child.pgid
      let descendants: ReadonlyArray<number> = []

      try {
        descendants = await waitFor(
          () => {
            const pids = listDescendants(child.pid)
            return pids.length === 2 ? pids : undefined
          },
          { message: "expected 2 direct descendants" },
        )

        process.kill(child.pid, "SIGKILL")
        await child.exited

        expect(await waitForDeath([child.pid])).toBe(true)
        const survivors = descendants.filter(isAlive)
        expect(survivors.length).toBeGreaterThan(0)
      } finally {
        killProcessGroup(pgid, "SIGKILL")
        await waitForDeath(descendants, { timeoutMs: 10_000 })
      }
    },
    20_000,
  )
})
