import { describe, expect, test } from "bun:test"
import { TASK_DIR, procAvailable, threadName, threadNames, waitForAnyThreadNamed, waitForThreadNamed } from "./thread-names"

const describeProc = procAvailable ? describe : describe.skip

describeProc("thread name inspection (/proc)", () => {
  test("reads the calling thread's comm", () => {
    // On Linux the main thread's tid equals the process id.
    const name = threadName(process.pid)
    expect(typeof name).toBe("string")
    expect(name!.length).toBeGreaterThan(0)
  })

  test("lists the main thread among the process threads", () => {
    expect(threadNames()).toContain(threadName(process.pid)!)
  })

  test("returns undefined for a tid that does not exist", () => {
    expect(threadName(2_147_483_647)).toBeUndefined()
  })

  test("waitForThreadNamed resolves for the current name and fails loudly otherwise", async () => {
    const name = threadName(process.pid)!
    await waitForThreadNamed(process.pid, name, 200)
    await expect(waitForThreadNamed(process.pid, "definitely-not-a-thread", 50)).rejects.toThrow(
      /never became "definitely-not-a-thread"/,
    )
  })

  test("waitForAnyThreadNamed resolves and reports what it saw on timeout", async () => {
    const name = threadName(process.pid)!
    await waitForAnyThreadNamed(name, 200)
    await expect(waitForAnyThreadNamed("definitely-not-a-thread", 50)).rejects.toThrow(
      `no thread named "definitely-not-a-thread" appeared in ${TASK_DIR}`,
    )
  })
})
