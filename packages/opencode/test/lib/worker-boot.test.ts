import { describe, expect, test } from "bun:test"
import { bootMany, maxOf, median, summarizeBootLevel } from "./worker-boot"

describe("worker-boot helpers", () => {
  test("median handles odd, even and empty samples without mutating input", () => {
    const odd = [3, 1, 2]
    expect(median(odd)).toBe(2)
    expect(odd).toEqual([3, 1, 2])
    expect(median([1, 2, 3, 4])).toBe(2.5)
    expect(median([])).toBe(0)
  })

  test("maxOf handles empty and non-empty samples", () => {
    expect(maxOf([])).toBe(0)
    expect(maxOf([4, 9, 2])).toBe(9)
  })

  test("summarizeBootLevel projects samples into a level with median and max", () => {
    const samples = [{ bootMs: 10 }, { bootMs: 30 }, { bootMs: 20 }]
    const level = summarizeBootLevel(3, samples, 1024)

    expect(level).toEqual({
      workers: 3,
      samples,
      bootMs: [10, 30, 20],
      medianBootMs: 20,
      maxBootMs: 30,
      rssBytes: 1024,
    })
  })

  test("bootMany collects samples in spawn order and honours its count", async () => {
    let calls = 0
    const samples = await bootMany(3, async () => ({ bootMs: ++calls }))

    expect(calls).toBe(3)
    expect(samples.map((sample) => sample.bootMs)).toEqual([1, 2, 3])
    expect(await bootMany(0, async () => ({ bootMs: 0 }))).toEqual([])
  })
})
