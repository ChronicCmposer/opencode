// The reference-only service set a session worker composes per location.
//
// Workers resolve `Project` from the hub snapshot and never run `git rev-parse`
// themselves. `Project` and `ProjectDirectories` are therefore absent: the map
// replaces `Project.node` internally with `snapshotProjectLayer`. Core `Git`
// (`GitV2`) and `RepositoryCache` stay because configured git references use
// them lazily.

import { AISDK } from "@opencode-ai/core/aisdk"
import { AgentV2 } from "@opencode-ai/core/agent"
import { Catalog } from "@opencode-ai/core/catalog"
import { CommandV2 } from "@opencode-ai/core/command"
import { Config } from "@opencode-ai/core/config"
import { Credential } from "@opencode-ai/core/credential"
import { Database } from "@opencode-ai/core/database/database"
import { DurableLog } from "@opencode-ai/core/durable-log"
import { EffectFlock } from "@opencode-ai/core/util/effect-flock"
import { EventV2 } from "@opencode-ai/core/event"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Git } from "@opencode-ai/core/git"
import { Global } from "@opencode-ai/core/global"
import { Integration } from "@opencode-ai/core/integration"
import { Location } from "@opencode-ai/core/location"
import { PluginV2 } from "@opencode-ai/core/plugin"
import { PluginInternal } from "@opencode-ai/core/plugin/internal"
import { Policy } from "@opencode-ai/core/policy"
import { Reference } from "@opencode-ai/core/reference"
import { RepositoryCache } from "@opencode-ai/core/repository-cache"
import { SkillV2 } from "@opencode-ai/core/skill"
import { SkillDiscovery } from "@opencode-ai/core/skill/discovery"

export const workerLocationServices = [
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
  PluginInternal.referenceOnlyNode,
  SkillV2.node,
  SkillDiscovery.node,
  Credential.node,
  Git.node,
  FSUtil.node,
  EffectFlock.node,
  Global.node,
  EventV2.node,
  DurableLog.node,
  Database.node,
  RepositoryCache.node,
] as const
