import { describe, expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { HubProxyRemoteError, createHubProxyClient, decodeHubProxyFrame, serveHubProxy } from "@/session/worker/hub-proxy-wire"
import { MAX_WORKER_FRAME_BYTES } from "@/session/worker/frame"
import {
  HubProxySpillMissingError,
  type HubProxySpill,
  type HubProxySpillReference,
} from "@/session/worker/hub-proxy-spill"

function fakeSpill() {
  const files = new Map<string, string>()
  const calls = { write: [] as string[], read: [] as string[], remove: [] as string[] }
  const key = (ref: HubProxySpillReference) => `${ref.__hubProxySpill.scope}/${ref.__hubProxySpill.hash}`
  const spill: HubProxySpill = {
    async write(body, scope, label) {
      const hash = createHash("sha256").update(body).digest("hex")
      const reference = { __hubProxySpill: { hash, bytes: Buffer.byteLength(body), scope } }
      files.set(key(reference), body)
      calls.write.push(label)
      return reference
    },
    async read(reference) {
      calls.read.push(key(reference))
      const body = files.get(key(reference))
      if (body === undefined) {
        throw new HubProxySpillMissingError({ hash: reference.__hubProxySpill.hash, scope: reference.__hubProxySpill.scope })
      }
      return body
    },
    async remove(reference) {
      calls.remove.push(key(reference))
      files.delete(key(reference))
    },
    async removeScope() {
      return 0
    },
    async sweep() {
      return 0
    },
  }
  return { spill, files, calls }
}

function setup(handlers: Parameters<typeof serveHubProxy>[0], store?: HubProxySpill, serverStore = store) {
  const channel = new MessageChannel()
  const posted: number[] = []
  for (const port of [channel.port1, channel.port2]) {
    const original = port.postMessage.bind(port)
    port.postMessage = (message: unknown, ...rest: never[]) => {
      if (typeof message === "string") posted.push(Buffer.byteLength(message))
      return original(message, ...rest)
    }
  }
  const scope = { spill: serverStore, spillScope: "worker-1" }
  const dispose = serveHubProxy(handlers, channel.port2, undefined, serverStore ? scope : {})
  const client = createHubProxyClient(channel.port1, store ? { spill: store, spillScope: "worker-1" } : {})
  return {
    client,
    posted,
    close: () => {
      dispose()
      client.dispose()
      channel.port1.close()
      channel.port2.close()
    },
  }
}

const big = (n = 6 * 1024 * 1024) => "x".repeat(n)

describe("hub-proxy spill", () => {
  test("oversized request reaches the handler identical and every frame stays under the cap", async () => {
    const { spill, files } = fakeSpill()
    let seen: unknown
    const h = setup({ messagesTransform: (...a) => ((seen = a), "ok") }, spill)
    const body = big()
    expect(await h.client.call("messagesTransform", [body])).toBe("ok")
    expect(seen).toEqual([body])
    expect(Math.max(...h.posted)).toBeLessThan(MAX_WORKER_FRAME_BYTES)
    expect(files.size).toBe(0)
    h.close()
  })

  test("oversized reply reaches the client identical and the reply file is removed", async () => {
    const { spill, files, calls } = fakeSpill()
    const body = big()
    const h = setup({ messagesTransform: () => ({ body }) }, spill)
    expect(await h.client.call("messagesTransform", [])).toEqual({ body })
    expect(Math.max(...h.posted)).toBeLessThan(MAX_WORKER_FRAME_BYTES)
    expect(calls.remove).toHaveLength(1)
    expect(files.size).toBe(0)
    h.close()
  })

  test("small calls never touch the spill", async () => {
    const { spill, calls } = fakeSpill()
    const h = setup({ messagesTransform: (...a) => a }, spill)
    expect(await h.client.call("messagesTransform", [1])).toEqual([1])
    expect(calls).toEqual({ write: [], read: [], remove: [] })
    h.close()
  })

  test("request file is removed after an error reply", async () => {
    const { spill, files } = fakeSpill()
    const h = setup({
      messagesTransform: () => {
        throw new Error("boom")
      },
    }, spill)
    await expect(h.client.call("messagesTransform", [big()])).rejects.toThrow("boom")
    expect(files.size).toBe(0)
    h.close()
  })

  test("request file is removed after dispose", async () => {
    const { spill, files } = fakeSpill()
    const h = setup({ messagesTransform: () => new Promise(() => {}) }, spill)
    const pending = h.client.call("messagesTransform", [big()])
    await Bun.sleep(50)
    expect(files.size).toBe(1)
    h.client.dispose()
    await expect(pending).rejects.toThrow("disposed")
    await Bun.sleep(10)
    expect(files.size).toBe(0)
    h.close()
  })

  test("request file is removed after a timeout", async () => {
    const { spill, files } = fakeSpill()
    const channel = new MessageChannel()
    serveHubProxy({ messagesTransform: () => new Promise(() => {}) }, channel.port2, undefined, { spill, spillScope: "worker-1" })
    const client = createHubProxyClient(channel.port1, { spill, spillScope: "worker-1", timeoutMs: 100 })
    await expect(client.call("messagesTransform", [big()])).rejects.toThrow("timed out")
    expect(files.size).toBe(0)
    client.dispose()
    channel.port1.close()
    channel.port2.close()
  })

  test("a missing request body fails only that call", async () => {
    const { spill, files } = fakeSpill()
    const h = setup({ messagesTransform: (...a) => a }, spill)
    const real = spill.write
    spill.write = async (...args) => {
      const ref = await real(...args)
      files.clear()
      return ref
    }
    await expect(h.client.call("messagesTransform", [big()])).rejects.toThrow('hub-proxy request "messagesTransform"')
    spill.write = real
    expect(await h.client.call("messagesTransform", [1])).toEqual([1])
    h.close()
  })

  test("a missing reply body fails only that call", async () => {
    const { spill, files } = fakeSpill()
    const clientStore = { ...spill, read: async (r: HubProxySpillReference) => (files.clear(), spill.read(r)) }
    const channel = new MessageChannel()
    serveHubProxy({ messagesTransform: (...a) => a[0] ?? big() }, channel.port2, undefined, {
      spill,
      spillScope: "worker-1",
    })
    const client = createHubProxyClient(channel.port1, { spill: clientStore, spillScope: "worker-1" })
    await expect(client.call("messagesTransform", [])).rejects.toThrow("spill body")
    expect(await client.call("messagesTransform", [2])).toBe(2)
    client.dispose()
    channel.port1.close()
    channel.port2.close()
  })

  test("without spill an oversized request names the method and byte count", async () => {
    const h = setup({ x: () => 1 } as never)
    const error = await h.client.call("x" as never, [big()]).catch((e: Error) => e)
    expect(error.message).toMatch(/hub-proxy request "x" is \d+ bytes/)
    h.close()
  })

  test("without spill an oversized reply yields an error naming the method", async () => {
    const h = setup({ x: () => big() } as never)
    const error = await h.client.call("x" as never, []).catch((e: Error) => e)
    expect(error).toBeInstanceOf(HubProxyRemoteError)
    expect(error.message).toContain('hub-proxy reply "x"')
    h.close()
  })

  test("a request whose spill reference has a non-hex hash is rejected at decode", () => {
    const frame = {
      kind: "request",
      id: 1,
      method: "x",
      args: [],
      spill: { __hubProxySpill: { hash: "zz", bytes: 1, scope: "worker-1" } },
    }
    expect(() => decodeHubProxyFrame(frame)).toThrow()
      })
})
