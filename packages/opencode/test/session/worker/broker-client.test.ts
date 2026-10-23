// Phase 10 / production wiring — worker-side broker client (plan step 18).
//
// The session worker reaches MCP/LSP through the hub's broker. `createWorkerBrokerClient`
// adapts a transferred `MessagePort` into the `BrokerClientHandle` seam (`call`) and the
// wider broker surface (`acquire`/`oauth`/`release`/`ping`/`cancel`/`teardown`), reusing
// the pure protocol parsers so a malformed response halts immediately.
//
// These tests drive the client against an in-process `MessageChannel` responder: no worker
// spawn, no child processes. They assert request framing, typed result parsing, coded
// failure propagation, disposal, and the handle-shape contract.

import { describe, expect, test } from "bun:test"
import { createWorkerBrokerClient } from "@/session/worker/broker-client"
import { workerLogMessage, type WorkerLogger } from "@/session/worker/log"
import type { BrokerClientHandle } from "@/session/worker/layer"
import {
  BrokerFailure,
  BrokerProtocolError,
  type BrokerRequest,
  type ResponseFrame,
} from "@/session/broker/protocol"

/** Answer frames on `port` with `responder`; returns a disposer. */
function serve(
  port: MessagePort,
  responder: (frame: BrokerRequest) => ResponseFrame | Promise<ResponseFrame>,
): () => void {
  const listener = (event: MessageEvent) => {
    const frame = event.data as BrokerRequest
    void Promise.resolve(responder(frame)).then(
      (response) => port.postMessage(response),
      () => undefined,
    )
  }
  port.addEventListener("message", listener)
  port.start()
  return () => port.removeEventListener("message", listener)
}

function channel(): MessageChannel {
  return new MessageChannel()
}

/** Assert `reason` is a coded broker failure and narrow it for field checks. */
function expectBrokerFailure(reason: unknown): BrokerFailure {
  expect(reason).toBeInstanceOf(BrokerFailure)
  if (!(reason instanceof BrokerFailure)) throw new Error("expected a BrokerFailure")
  return reason
}

describe("createWorkerBrokerClient (step 18)", () => {
  test("call forwards a framed request and resolves the response value", async () => {
    const { port1, port2 } = channel()
    const seen: BrokerRequest[] = []
    const stop = serve(port1, (frame) => {
      seen.push(frame)
      return { kind: "response", requestId: frame.requestId, ok: true, value: { echoed: true } }
    })

    const client = createWorkerBrokerClient(port2)
    const result = await client.call("/work/a", "fake", "tools/list", { cursor: 1 })

    expect(result).toEqual({ echoed: true })
    expect(seen).toHaveLength(1)
    expect(seen[0]).toMatchObject({
      kind: "call",
      directory: "/work/a",
      server: "fake",
      method: "tools/list",
      params: { cursor: 1 },
    })
    expect(typeof seen[0]?.requestId).toBe("string")

    client.dispose()
    stop()
  })

  test("satisfies the BrokerClientHandle seam in layer.ts", () => {
    const { port2 } = channel()
    const client = createWorkerBrokerClient(port2)
    const handle: BrokerClientHandle = client
    expect(typeof handle.call).toBe("function")
    client.dispose()
  })

  test("parses typed acquire/oauth/ping responses with the protocol parsers", async () => {
    const { port1, port2 } = channel()
    const stop = serve(port1, (frame) => {
      switch (frame.kind) {
        case "acquire":
          return {
            kind: "response",
            requestId: frame.requestId,
            ok: true,
            value: { instanceId: "inst-1", servers: [{ name: "fake", pid: 7, pgid: 7, instanceId: "inst-1" }] },
          }
        case "oauth":
          return {
            kind: "response",
            requestId: frame.requestId,
            ok: true,
            value: { callbackUrl: "opencode://mcp/oauth/inst-1", instanceId: "inst-1", owner: "broker" },
          }
        default:
          return { kind: "response", requestId: frame.requestId, ok: true, value: { threadId: 3, directories: 2 } }
      }
    })

    const client = createWorkerBrokerClient(port2)
    const acquired = await client.acquire({ directory: "/work/a", servers: [{ name: "fake", kind: "mcp", command: ["x"] }] })
    expect(acquired.instanceId).toBe("inst-1")
    expect(acquired.servers[0]?.pid).toBe(7)

    const oauth = await client.oauth("/work/a")
    expect(oauth).toEqual({ callbackUrl: "opencode://mcp/oauth/inst-1", instanceId: "inst-1", owner: "broker" })

    const ping = await client.ping()
    expect(ping).toEqual({ threadId: 3, directories: 2 })

    client.dispose()
    stop()
  })

  test("rejects with a coded BrokerFailure on an error response", async () => {
    const { port1, port2 } = channel()
    const stop = serve(port1, (frame) => ({
      kind: "response",
      requestId: frame.requestId,
      ok: false,
      error: { code: "timeout", message: "call timed out" },
    }))

    const client = createWorkerBrokerClient(port2)
    const error = expectBrokerFailure(await client.call("/work/a", "fake", "hang").catch((reason: unknown) => reason))

    expect(error.code).toBe("timeout")
    expect(error.message).toBe("call timed out")

    client.dispose()
    stop()
  })

  test("rejects when a typed response value is malformed", async () => {
    const { port1, port2 } = channel()
    const stop = serve(port1, (frame) => ({
      kind: "response",
      requestId: frame.requestId,
      ok: true,
      value: { instanceId: "inst-1", servers: "not-an-array" },
    }))

    const client = createWorkerBrokerClient(port2)
    const error = await client
      .acquire({ directory: "/work/a", servers: [{ name: "fake", kind: "mcp", command: ["x"] }] })
      .catch((reason: unknown) => reason)

    expect(error).toBeInstanceOf(BrokerProtocolError)

    client.dispose()
    stop()
  })

  test("dispose rejects in-flight requests and drops later replies", async () => {
    const { port1, port2 } = channel()
    // A responder that never answers keeps the request pending.
    const stop = serve(port1, () => new Promise<ResponseFrame>(() => {}))

    const client = createWorkerBrokerClient(port2)
    const pending = client.call("/work/a", "fake", "slow")
    client.dispose()

    const error = expectBrokerFailure(await pending.catch((reason: unknown) => reason))
    expect(error.code).toBe("crashed")

    const afterDispose = expectBrokerFailure(await client.call("/work/a", "fake", "later").catch((reason: unknown) => reason))
    expect(afterDispose.code).toBe("crashed")

    stop()
  })

  test("times out an unanswered request when a timeout is configured", async () => {
    const { port1, port2 } = channel()
    const stop = serve(port1, () => new Promise<ResponseFrame>(() => {}))

    const client = createWorkerBrokerClient(port2, { timeoutMs: 30 })
    const error = expectBrokerFailure(await client.call("/work/a", "fake", "slow").catch((reason: unknown) => reason))

    expect(error.code).toBe("timeout")

    client.dispose()
    stop()
  })
})

// ---------------------------------------------------------------------------
// E2 — peer lifecycle logging. The client emits a structured connect line at
// construction and a close line at dispose, so a hub-side reader can correlate
// the worker's broker peer with its pool slot. A construction failure emits a
// WARN connect-failed line (with the error `reason`) before rethrowing.
// ---------------------------------------------------------------------------

interface RecordedLog {
  readonly message: string
  readonly fields: Record<string, unknown>
}

function recordingLogger(records: RecordedLog[]): WorkerLogger {
  return (message, fields) => void records.push({ message, fields })
}

describe("worker broker client lifecycle logging (E2)", () => {
  test("emits an INFO connect line with the peer and workerID", () => {
    const { port2 } = channel()
    const records: RecordedLog[] = []
    const client = createWorkerBrokerClient(port2, { log: recordingLogger(records), workerID: 7 })

    expect(records).toEqual([
      { message: workerLogMessage("broker connected"), fields: { peer: "broker", level: "info", workerID: 7 } },
    ])
    client.dispose()
  })

  test("emits an INFO close line on dispose", () => {
    const { port2 } = channel()
    const records: RecordedLog[] = []
    const client = createWorkerBrokerClient(port2, { log: recordingLogger(records), workerID: 7 })
    client.dispose()

    expect(records.at(-1)).toEqual({
      message: workerLogMessage("broker closed"),
      fields: { peer: "broker", level: "info", workerID: 7 },
    })
  })

  test("omits workerID when the caller did not supply one", () => {
    const { port2 } = channel()
    const records: RecordedLog[] = []
    const client = createWorkerBrokerClient(port2, { log: recordingLogger(records) })

    expect(records[0]?.fields).toEqual({ peer: "broker", level: "info" })
    client.dispose()
  })

  test("a failed connect emits a WARN line with the reason and rethrows", () => {
    const records: RecordedLog[] = []
    const brokenPort = {
      addEventListener: () => {},
      removeEventListener: () => {},
      start: () => {
        throw new Error("port closed")
      },
    } as unknown as MessagePort

    expect(() => createWorkerBrokerClient(brokenPort, { log: recordingLogger(records), workerID: 3 })).toThrow(
      "port closed",
    )
    expect(records).toEqual([
      {
        message: workerLogMessage("broker connect failed"),
        fields: { peer: "broker", level: "warn", reason: "port closed", workerID: 3 },
      },
    ])
  })
})
