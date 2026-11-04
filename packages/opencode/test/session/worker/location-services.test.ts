// Worker location-map wiring.
//
// The worker composes a reference-only service set: `Project` is replaced from
// the hub snapshot, there is no watcher, and the map shares the worker database
// instead of owning a second client.

import { describe, expect, test } from "bun:test"
import { Duration, Effect } from "effect"
import { Config as CoreConfig } from "@opencode-ai/core/config"
import { Database } from "@opencode-ai/core/database/database"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { EventV2 } from "@opencode-ai/core/event"
import { Global } from "@opencode-ai/core/global"
import {
  isWorkerLocationServiceMapNode,
  type WorkerLocationServiceMapNode,
} from "@opencode-ai/core/location-services"
import { Project } from "@opencode-ai/core/project"
import {
  MissingWorkerDatabaseError,
  missingWorkerDatabaseLayer,
  type WorkerDatabase,
} from "@/session/worker/db"
import {
  boundaryReplacements,
  workerLocationMapIdleTimeToLive,
  workerLocationServiceMapNode,
} from "@/session/worker/layer"
import { workerLocationServices } from "@/session/worker/location-services"
import { workerSnapshot } from "../../lib/snapshot"

const fakeDatabase = (): WorkerDatabase =>
  ({
    eventBinding: EventV2.makeWorkerScope(),
    durableLog: {},
  }) as unknown as WorkerDatabase

const names = (map: WorkerLocationServiceMapNode): readonly string[] =>
  map.locationReplacements.map(([source]) => source.name)

describe("workerLocationServiceMapNode", () => {
  test("builds a branded worker location map", () => {
    const map = workerLocationServiceMapNode(workerSnapshot())

    expect(isWorkerLocationServiceMapNode(map)).toBe(true)
    expect(isWorkerLocationServiceMapNode(undefined)).toBe(false)
    expect(isWorkerLocationServiceMapNode({ kind: "layer", name: map.name })).toBe(false)
  })

  test("top-level allowlist excludes Project and ProjectDirectories", () => {
    const locationNames = workerLocationServices.map((node) => node.name)

    expect(locationNames).not.toContain(Project.node.name)
    expect(locationNames).not.toContain("@opencode/ProjectDirectories")
  })

  test("internally replaces Project, core Config, Database and Global", () => {
    const map = workerLocationServiceMapNode(workerSnapshot())

    expect(names(map)).toContain(Project.node.name)
    expect(names(map)).toContain(CoreConfig.node.name)
    expect(names(map)).toContain(Database.node.name)
    expect(names(map)).toContain(Global.node.name)
    expect(names(map)).not.toContain(DurableLog.node.name)
  })

  test("adds DurableLog and the worker scope when a database is present", () => {
    const database = fakeDatabase()
    const map = workerLocationServiceMapNode(workerSnapshot(), database)

    expect(names(map)).toContain(DurableLog.node.name)
    expect(map.locationContext).toBeDefined()
  })

  test("locations never idle out", () => {
    expect(Duration.toMillis(workerLocationMapIdleTimeToLive)).toBe(Number.POSITIVE_INFINITY)
  })

  test("a missing database fails only when the map is used, not at construction", async () => {
    expect(() => boundaryReplacements(workerSnapshot())).not.toThrow()
    const map = workerLocationServiceMapNode(workerSnapshot())
    const database = map.locationReplacements.find(([source]) => source.name === Database.node.name)?.[1]
    expect(database).toBeDefined()

    const error = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* Database.Service
      }).pipe(Effect.provide(missingWorkerDatabaseLayer())),
    ).catch((cause: unknown) => cause)

    expect(error).toBeInstanceOf(MissingWorkerDatabaseError)
  })
})
