import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import type { CancelOutcome } from "@/session/worker/cancel"
import { cancelSessionTree } from "@/session/prompt"
import type { SessionID } from "@/session/schema"

const id = (value: string) => value as SessionID

describe("cancelSessionTree", () => {
  test("cancels task, delegate and metadata descendants recursively", async () => {
    // `delegate` carries depth 0 but is still a Session.children edge, so it must
    // cancel with the rest of the tree.
    const tree: Record<string, string[]> = {
      root: ["task", "delegate"],
      task: ["metadata-grandchild", "task-grandchild"],
      delegate: [],
      "metadata-grandchild": [],
      "task-grandchild": [],
    }
    const cancelled: string[] = []
    const outcome = await Effect.runPromise(
      cancelSessionTree({
        children: (sessionID) => Effect.succeed((tree[sessionID] ?? []).map((child) => ({ id: id(child) }))),
        cancelOne: (sessionID) =>
          Effect.sync(() => {
            cancelled.push(sessionID)
            return undefined
          }),
      })(id("root")),
    )

    expect(outcome).toBeUndefined()
    expect(cancelled).toHaveLength(5)
    expect(cancelled).toContain("root")
    expect(cancelled).toContain("task")
    expect(cancelled).toContain("delegate")
    expect(cancelled).toContain("metadata-grandchild")
    expect(cancelled).toContain("task-grandchild")
    // Children-first: the root is the last cancellation.
    expect(cancelled.at(-1)).toBe("root")
    expect(cancelled.indexOf("task-grandchild")).toBeLessThan(cancelled.indexOf("task"))
  })

  test("returns the root outcome and cancels a depth-0 delegate lineage", async () => {
    const cancelled: string[] = []
    const rootOutcome: CancelOutcome = {
      sessionID: id("root"),
      mode: "terminated",
      winner: "cancel",
      acknowledged: false,
      durationMs: 0,
    }
    const outcome = await Effect.runPromise(
      cancelSessionTree({
        children: (sessionID) =>
          Effect.succeed(sessionID === id("root") ? [{ id: id("delegate") }, { id: id("task") }] : []),
        cancelOne: (sessionID) =>
          Effect.sync(() => {
            cancelled.push(sessionID)
            return sessionID === id("root") ? rootOutcome : undefined
          }),
      })(id("root")),
    )

    expect(outcome).toBe(rootOutcome)
    expect(cancelled).toEqual(["delegate", "task", "root"])
  })

  test("a cyclic parent edge terminates and cancels each session once", async () => {
    const tree: Record<string, string[]> = {
      a: ["b"],
      b: ["a"],
    }
    const cancelled: string[] = []
    await Effect.runPromise(
      cancelSessionTree({
        children: (sessionID) => Effect.succeed((tree[sessionID] ?? []).map((child) => ({ id: id(child) }))),
        cancelOne: (sessionID) =>
          Effect.sync(() => {
            cancelled.push(sessionID)
            return undefined
          }),
      })(id("a")),
    )

    expect(cancelled).toEqual(["b", "a"])
  })
})
