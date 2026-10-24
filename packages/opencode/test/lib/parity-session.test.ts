import { describe, expect, test } from "bun:test"
import { tmpdir } from "../fixture/fixture"
import { parseGoldenReference, readGolden, runParitySession } from "./parity-session"

describe("parity golden fixtures", () => {
  test("fails loud when the fixture is missing", async () => {
    await using tmp = await tmpdir()
    await expect(readGolden(`${tmp.path}/missing.json`)).rejects.toThrow(/golden fixture not found/)
  })

  test("rejects a fixture with no events array", async () => {
    await using tmp = await tmpdir()
    const path = `${tmp.path}/golden.json`
    await Bun.write(path, JSON.stringify({ name: "x" }))
    await expect(readGolden(path)).rejects.toThrow(/has no "events" array/)
  })

  test("rejects a fixture with a malformed event", async () => {
    await using tmp = await tmpdir()
    const path = `${tmp.path}/golden.json`
    await Bun.write(path, JSON.stringify({ name: "x", events: [{ type: 1 }] }))
    await expect(readGolden(path)).rejects.toThrow(/event #0 needs string "id" and "type"/)
  })

  test("parseGoldenReference accepts a well-formed fixture", () => {
    const reference = parseGoldenReference({
      name: "n",
      events: [{ id: "evt_1", type: "t", properties: { a: 1 } }],
    })
    expect(reference.events).toEqual([{ id: "evt_1", type: "t", properties: { a: 1 } }])
  })
})

describe("parity run cancellation", () => {
  test("aborts a timed-out run's signal so the runner stops instead of leaking", async () => {
    const observedAborted: boolean[] = []
    const result = await runParitySession({
      script: { name: "cancel", prompt: "never" },
      timeoutMs: 40,
      run: (context) =>
        new Promise<void>((resolve) => {
          context.signal.addEventListener(
            "abort",
            () => {
              observedAborted.push(context.signal.aborted)
              resolve()
            },
            { once: true },
          )
        }),
    })

    expect(result.off.error).toContain("timed out")
    expect(result.on.error).toContain("timed out")
    expect(observedAborted).toEqual([true, true])
  })
})
