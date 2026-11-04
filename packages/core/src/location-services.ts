import { Duration, Effect, Layer, LayerMap } from "effect"
import { AgentV2 } from "./agent"
import { AISDK } from "./aisdk"
import { Catalog } from "./catalog"
import { CommandV2 } from "./command"
import { Config } from "./config"
import { LayerNode } from "./effect/layer-node"
import { Node } from "./effect/app-node"
import { FileMutation } from "./file-mutation"
import { FileSystem } from "./filesystem"
import { FileSystemSearch } from "./filesystem/search"
import { Watcher } from "./filesystem/watcher"
import { Image } from "./image"
import { Integration } from "./integration"
import { Location } from "./location"
import { LocationMutation } from "./location-mutation"
import { LocationServiceMap } from "./location-service-map"
import { PermissionV2 } from "./permission"
import { PluginV2 } from "./plugin"
import { PluginInternal } from "./plugin/internal"
import { Policy } from "./policy"
import { ProjectCopy } from "./project/copy"
import { Pty } from "./pty"
import { QuestionV2 } from "./question"
import { Reference } from "./reference"
import { ReferenceGuidance } from "./reference/guidance"
import * as SessionRunnerLLM from "./session/runner/llm"
import { SessionRunnerModel } from "./session/runner/model"
import { SessionTodo } from "./session/todo"
import { SkillV2 } from "./skill"
import { SkillGuidance } from "./skill/guidance"
import { Snapshot } from "./snapshot"
import { SystemContextBuiltIns } from "./system-context/builtins"
import { SystemContextRegistry } from "./system-context/registry"
import { BuiltInTools } from "./tool/builtins"
import { ReadToolFileSystem } from "./tool/read-filesystem"
import { ToolRegistry } from "./tool/registry"
import { ToolOutputStore } from "./tool-output-store"

export { LocationServiceMap } from "./location-service-map"

export const locationServices = LayerNode.group([
  Location.node,
  Policy.node,
  Config.node,
  AgentV2.node,
  CommandV2.node,
  Reference.node,
  Integration.node,
  Catalog.node,
  AISDK.node,
  PluginV2.node,
  PluginInternal.node,
  ProjectCopy.node,
  ProjectCopy.refreshNode,
  FileSystemSearch.node,
  FileSystem.node,
  Watcher.node,
  Pty.node,
  SkillV2.node,
  SystemContextRegistry.node,
  SystemContextBuiltIns.node,
  LocationMutation.node,
  FileMutation.node,
  PermissionV2.node,
  ToolOutputStore.node,
  ToolRegistry.node,
  ToolRegistry.toolsNode,
  Image.node,
  SkillGuidance.node,
  ReferenceGuidance.node,
  SessionTodo.node,
  QuestionV2.node,
  ReadToolFileSystem.node,
  BuiltInTools.node,
  SessionRunnerModel.node,
  Snapshot.node,
  SessionRunnerLLM.node,
])

export type LocationServices = LayerNode.Output<typeof locationServices>
export type LocationError = LayerNode.Error<typeof locationServices>

type FullLocationMap = LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>
type GlobalTag = NonNullable<typeof LocationServiceMap.node.tag>

const WorkerLocationMapBrand: unique symbol = Symbol("opencode/WorkerLocationServiceMap")
const emptyContext = Layer.empty as Layer.Layer<unknown, never, never>

export interface LocationServiceMapOptions {
  readonly idleTimeToLive?: Duration.Input
}

export interface LocationServiceMapNode extends LayerNode.Node<LocationServiceMap.Service, never, GlobalTag> {
  readonly locationNodes: readonly LayerNode.AnyNode[]
  readonly locationReplacements: LayerNode.Replacements
  readonly locationContext: Layer.Layer<unknown, never, never>
}

export interface WorkerLocationServiceMapNode extends LocationServiceMapNode {
  readonly [WorkerLocationMapBrand]: true
}

export function isWorkerLocationServiceMapNode(input: unknown): input is WorkerLocationServiceMapNode {
  return (
    typeof input === "object" &&
    input !== null &&
    (input as { readonly [WorkerLocationMapBrand]?: unknown })[WorkerLocationMapBrand] === true
  )
}

export function buildLocationServiceMapFrom(
  services: readonly [LayerNode.AnyNode, ...LayerNode.AnyNode[]],
  replacements: LayerNode.Replacements = [],
  context: Layer.Layer<unknown, never, never> = emptyContext,
  options: LocationServiceMapOptions = {},
): Layer.Layer<LocationServiceMap.Service> {
  const group = LayerNode.group(services)

  const map = LayerMap.make(
    (ref: Location.Ref) => {
      const allReplacements = replacements.concat([[Location.node, Location.boundNode(ref)]])
      // Apply replacements during hoist, not afterward: replacements can
      // introduce new tagged dependencies (Location.boundNode depends on
      // Project), and the hoist walk is the only pass that can still slice
      // those back out.
      const location = LayerNode.hoist(group, Node.tags.values.global, allReplacements)

      return LayerNode.compile(location.node).pipe(
        Layer.fresh,
        Layer.tap(() =>
          Effect.logInfo("booting location services", {
            directory: ref.directory,
            workspaceID: ref.workspaceID,
          }),
        ),
        Layer.provide(LayerNode.compile(location.hoisted)),
        Layer.provide(context),
      )
    },
    options.idleTimeToLive === undefined ? {} : { idleTimeToLive: options.idleTimeToLive },
  )

  // Keep the public service type as the full map for existing hub consumers.
  return Layer.effect(LocationServiceMap.Service, map as unknown as Effect.Effect<FullLocationMap>)
}

export function buildLocationServiceMap(
  replacements: LayerNode.Replacements = [],
): Layer.Layer<LocationServiceMap.Service> {
  return buildLocationServiceMapFrom(
    locationServices.dependencies as readonly [LayerNode.AnyNode, ...LayerNode.AnyNode[]],
    replacements,
    emptyContext,
    { idleTimeToLive: "60 minutes" },
  )
}

export function buildLocationServiceMapNode(
  services: readonly [LayerNode.AnyNode, ...LayerNode.AnyNode[]],
  replacements: LayerNode.Replacements = [],
  context: Layer.Layer<unknown, never, never> = emptyContext,
  options: LocationServiceMapOptions = {},
): WorkerLocationServiceMapNode {
  return {
    kind: "layer",
    name: LocationServiceMap.node.name,
    service: LocationServiceMap.Service,
    implementation: buildLocationServiceMapFrom(services, replacements, context, options),
    dependencies: [],
    tag: LocationServiceMap.node.tag,
    locationNodes: services,
    locationReplacements: replacements,
    locationContext: context,
    [WorkerLocationMapBrand]: true,
  }
}

// This is temporary for backwards compatibility
export const locationServiceMapLayer = buildLocationServiceMap()
