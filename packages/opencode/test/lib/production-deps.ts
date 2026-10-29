// Shared dependency-free replacements for the production session-hub node.
//
// `productionSessionHubNode()` declares ten service deps (Config, Auth,
// ModelsDev, Provider, RuntimeFlags, SessionStatus, Permission, Question,
// EventV2Bridge, RunBarrier). Suites that only exercise the node's wiring replaced them one
// by one, which drifted between files. This module is the single source: each
// entry is a dependency-free mock (or, for RuntimeFlags, the real layer) so the
// node builds without touching the filesystem, network, or a real hub.
//
// Callers may override any service's mock methods; the defaults exist so a
// wiring-only test needs no setup at all.

import { Effect, Layer } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { ModelsDev } from "@opencode-ai/core/models-dev"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Auth } from "@/auth"
import { Config } from "@/config/config"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Permission } from "@/permission"
import { Question } from "@/question"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { Provider } from "@/provider/provider"
import { SessionStatus } from "@/session/status"
import { RunBarrier } from "@/session/run-barrier"

/** Per-service overrides for {@link productionServiceReplacements}. */
export interface ProductionServiceDepsOptions {
  readonly config?: Partial<Config.Interface>
  readonly auth?: Partial<Auth.Interface>
  readonly modelsDev?: Partial<ModelsDev.Interface>
  readonly provider?: Partial<Provider.Interface>
  readonly flags?: Partial<RuntimeFlags.Info>
  readonly sessionStatus?: Partial<SessionStatus.Interface>
  readonly permission?: Partial<Permission.Interface>
  readonly question?: Partial<Question.Interface>
  readonly eventV2Bridge?: Partial<EventV2Bridge.Interface>
}

/**
 * The replacement list for `productionSessionHubNode`'s declared service deps.
 *
 * Every default is inert: `Config`/`Auth`/`Provider` return fixed fixtures,
 * `ModelsDev` returns an empty catalog, the hub authorities are unimplemented
 * mocks, and the bridge's live publish is a no-op. A test that needs behavior
 * supplies only the methods it exercises through the matching option.
 */
export function productionServiceReplacements(
  options: ProductionServiceDepsOptions = {},
): LayerNode.Replacements {
  return [
    [
      Config.node,
      Layer.mock(Config.Service, {
        get: () => Effect.succeed({ model: "test/model" }),
        directories: () => Effect.succeed(["/cfg/opencode"]),
        ...options.config,
      }),
    ],
    [
      Auth.node,
      Layer.mock(Auth.Service, {
        get: () => Effect.succeed(new Auth.Api({ type: "api", key: "sk-test" })),
        all: () => Effect.succeed({ test: new Auth.Api({ type: "api", key: "sk-test" }) }),
        ...options.auth,
      }),
    ],
    [ModelsDev.node, Layer.mock(ModelsDev.Service, { get: () => Effect.succeed({}), ...options.modelsDev })],
    [
      Provider.node,
      Layer.mock(Provider.Service, {
        defaultModel: () =>
          Effect.succeed({ providerID: ProviderV2.ID.make("test"), modelID: ModelV2.ID.make("model") }),
        ...options.provider,
      }),
    ],
    [RuntimeFlags.node, RuntimeFlags.layer({ experimentalSessionWorkers: true, ...options.flags })],
    [SessionStatus.node, Layer.mock(SessionStatus.Service, { ...options.sessionStatus })],
    [Permission.node, Layer.mock(Permission.Service, { ...options.permission })],
    [Question.node, Layer.mock(Question.Service, { ...options.question })],
    [
      EventV2Bridge.node,
      Layer.mock(EventV2Bridge.Service, { publishLive: () => Effect.void, ...options.eventV2Bridge }),
    ],
    [RunBarrier.node, RunBarrier.layer],
  ]
}
