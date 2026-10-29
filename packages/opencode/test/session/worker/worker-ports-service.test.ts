// The worker-ports service is the one authority for hub-side port attachment.
// These tests pin the `Interface` / `make` / `layer` / `node` shape and the
// synchronous attachments facade the Promise-shaped hub composes.

import { describe, expect, test } from "bun:test"
import { Effect, Tracer } from "effect"
import { createHubProxyClient } from "@/session/worker/hub-proxy-wire"
import type { PidReport } from "@/session/worker/pid-registry"
import { Service, layer, make, makeAttachments, node, type WorkerPorts } from "@/session/worker/worker-ports"
import { it } from "../../lib/effect"

const pidReport = (workerID: number, pgid: number): PidReport => ({
  type: "pid-report",
  workerID,
  entries: [{ pid: pgid, pgid, label: "shell", workerID }],
})

/** Poll a synchronous predicate, yielding to the real event loop between checks. */
const waitUntil = (predicate: () => boolean, message: string) =>
  Effect.gen(function* () {
    while (!predicate()) yield* Effect.sleep("5 millis")
  }).pipe(
    Effect.timeoutOrElse({
      duration: "2 seconds",
      orElse: () => Effect.fail(new Error(message)),
    }),
  )

const messageChannel = () => new MessageChannel()

describe("worker-ports service", () => {
  it.live("attaches, reattaches and disposes idempotently", () =>
    Effect.gen(function* () {
      let disposals = 0
      const ports = yield* make({
        backends: { egress: () => () => void disposals++ },
      })
      const first = messageChannel()
      const second = messageChannel()

      const disposeFirst = yield* ports.attachWorkerPorts(1, { egress: first.port1 })
      expect(typeof disposeFirst).toBe("function")

      // A re-attach disposes the previous server for the same worker.
      const disposeSecond = yield* ports.attachWorkerPorts(1, { egress: second.port1 })
      expect(disposals).toBe(1)

      disposeSecond()
      disposeSecond()
      expect(disposals).toBe(2)

      yield* ports.disposeWorker(1)
      yield* ports.disposeWorker(1)
      expect(disposals).toBe(2)

      expect(yield* ports.attached()).toEqual({ workerPorts: [], hubProxy: [] })
      first.port1.close()
      first.port2.close()
      second.port1.close()
      second.port2.close()
    }),
  )

  it.live("resolves the backend source once across attachments", () =>
    Effect.gen(function* () {
      let resolutions = 0
      const ports = yield* make({
        backends: () => {
          resolutions++
          return { egress: () => () => {} }
        },
      })
      const first = messageChannel()
      const second = messageChannel()

      yield* ports.attachWorkerPorts(1, { egress: first.port1 })
      yield* ports.attachWorkerPorts(2, { egress: second.port1 })
      expect(resolutions).toBe(1)

      yield* ports.disposeAll()
      first.port1.close()
      first.port2.close()
      second.port1.close()
      second.port2.close()
    }),
  )

  it.live("is a no-op when no backends are configured", () =>
    Effect.gen(function* () {
      const ports = yield* make({})
      const channel = messageChannel()

      const dispose = yield* ports.attachWorkerPorts(1, { egress: channel.port1 })
      dispose()
      dispose()

      const disposeProxy = yield* ports.attachHubProxyPort(1, channel.port2)
      disposeProxy()

      yield* ports.disposeWorker(1)
      yield* ports.disposeAll()
      expect(yield* ports.attached()).toEqual({ workerPorts: [], hubProxy: [] })

      channel.port1.close()
      channel.port2.close()
    }),
  )

  it.live("serves pid reports and hub-proxy requests on the same control port", () =>
    Effect.gen(function* () {
      const reports: PidReport[] = []
      const ports = yield* make({
        backends: { hubProxy: { "sessionStatus.get": () => "pong" } },
        onPidReport: (workerID, report) => {
          expect(workerID).toBe(7)
          reports.push(report)
        },
      })
      const channel = messageChannel()
      const dispose = yield* ports.attachHubProxyPort(7, channel.port1)
      const client = createHubProxyClient(channel.port2)

      expect(yield* Effect.promise(() => client.call("sessionStatus.get", []))).toBe("pong")

      channel.port2.postMessage(pidReport(7, 4242))
      yield* waitUntil(() => reports.length === 1, "pid report was never received")
      expect(reports[0]?.entries[0]?.pgid).toBe(4242)

      // The pid frame must not tear down the hub-proxy listener on the same port.
      expect(yield* Effect.promise(() => client.call("sessionStatus.get", []))).toBe("pong")

      client.dispose()
      dispose()
      channel.port1.close()
      channel.port2.close()
    }),
  )

  it.live("does not close a caller's port when its server is disposed", () =>
    Effect.gen(function* () {
      const ports = yield* make({
        backends: { hubProxy: { "sessionStatus.get": () => "pong" } },
      })
      const channel = messageChannel()
      const dispose = yield* ports.attachHubProxyPort(1, channel.port1)
      dispose()

      const received: unknown[] = []
      channel.port1.addEventListener("message", (event) => received.push(event.data))
      channel.port1.start()
      channel.port2.postMessage("still-open")

      yield* waitUntil(() => received.length === 1, "caller port was closed by dispose")
      expect(received).toEqual(["still-open"])

      channel.port1.close()
      channel.port2.close()
    }),
  )

  it.live("reports the workers with attached ports", () =>
    Effect.gen(function* () {
      const ports = yield* make({
        backends: { egress: () => () => {}, hubProxy: {} },
        onPidReport: () => {},
      })
      const egress = messageChannel()
      const control = messageChannel()

      yield* ports.attachWorkerPorts(1, { egress: egress.port1 })
      const disposeProxy = yield* ports.attachHubProxyPort(2, control.port1)
      expect(yield* ports.attached()).toEqual({ workerPorts: [1], hubProxy: [2] })

      disposeProxy()
      expect(yield* ports.attached()).toEqual({ workerPorts: [1], hubProxy: [] })

      yield* ports.disposeAll()
      expect(yield* ports.attached()).toEqual({ workerPorts: [], hubProxy: [] })

      egress.port1.close()
      egress.port2.close()
      control.port1.close()
      control.port2.close()
    }),
  )

  it.live("installs a WorkerPorts through the layer", () =>
    Effect.gen(function* () {
      const ports: WorkerPorts = yield* Service
      expect(typeof ports.attachWorkerPorts).toBe("function")
      expect(yield* ports.attached()).toEqual({ workerPorts: [], hubProxy: [] })
    }).pipe(Effect.provide(layer({}))),
  )

  test("node is an unbound LayerNode for the service", () => {
    const entry = node({})
    expect(entry.kind).toBe("layer")
    expect(entry.name).toBe("@opencode/WorkerPorts")
    expect(entry.service).toBe(Service)
    expect(entry.dependencies).toEqual([])
  })

  it.live("names every service method with its WorkerPorts span", () => {
    const names: string[] = []
    const tracer = Tracer.make({
      span(options) {
        names.push(options.name)
        return new Tracer.NativeSpan(options)
      },
    })

    return Effect.gen(function* () {
      let disposals = 0
      const ports = yield* make({ backends: { egress: () => () => void disposals++, hubProxy: {} } })
      const egress = messageChannel()
      const control = messageChannel()

      const disposePorts = yield* ports.attachWorkerPorts(1, { egress: egress.port1 })
      const disposeProxy = yield* ports.attachHubProxyPort(2, control.port1)
      expect(yield* ports.attached()).toEqual({ workerPorts: [1], hubProxy: [2] })

      disposePorts()
      yield* ports.disposeWorker(2)
      yield* ports.disposeAll()
      disposeProxy()
      expect(disposals).toBe(1)
      expect(yield* ports.attached()).toEqual({ workerPorts: [], hubProxy: [] })

      egress.port1.close()
      egress.port2.close()
      control.port1.close()
      control.port2.close()
    }).pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.map(() =>
        expect(names).toEqual(
          expect.arrayContaining([
            "WorkerPorts.make",
            "WorkerPorts.attachWorkerPorts",
            "WorkerPorts.attachHubProxyPort",
            "WorkerPorts.attached",
            "WorkerPorts.disposeWorker",
            "WorkerPorts.disposeAll",
          ]),
        ),
      ),
    )
  })

  it.live("matches the attachments facade", () =>
    Effect.gen(function* () {
      const makeOptions = () => {
        let resolutions = 0
        let disposals = 0
        return {
          resolutions: () => resolutions,
          disposals: () => disposals,
          options: {
            backends: () => {
              resolutions++
              return { egress: () => () => void disposals++ }
            },
            hubProxyHandlersFor: () => ({ "sessionStatus.get": () => "pong" }),
            hubProxyStreamingFor: () => undefined,
          },
        }
      }

      const serviceBase = makeOptions()
      const service = yield* make(serviceBase.options)
      const serviceChannel = messageChannel()
      const serviceNext = messageChannel()
      const serviceFirst = yield* service.attachWorkerPorts(1, { egress: serviceChannel.port1 })
      const serviceSecond = yield* service.attachWorkerPorts(1, { egress: serviceNext.port1 })
      serviceSecond()
      serviceFirst()
      expect(serviceBase.resolutions()).toBe(1)
      expect(serviceBase.disposals()).toBe(2)

      const facadeBase = makeOptions()
      const facade = makeAttachments(facadeBase.options)
      const facadeChannel = messageChannel()
      const facadeNext = messageChannel()
      const facadeFirst = facade.attachWorkerPorts(1, { egress: facadeChannel.port1 })
      const facadeSecond = facade.attachWorkerPorts(1, { egress: facadeNext.port1 })
      facadeSecond()
      facadeFirst()
      expect(facadeBase.resolutions()).toBe(1)
      expect(facadeBase.disposals()).toBe(2)

      yield* service.disposeAll()
      facade.disposeAll()

      serviceChannel.port1.close()
      serviceChannel.port2.close()
      serviceNext.port1.close()
      serviceNext.port2.close()
      facadeChannel.port1.close()
      facadeChannel.port2.close()
      facadeNext.port1.close()
      facadeNext.port2.close()
    }),
  )
})
