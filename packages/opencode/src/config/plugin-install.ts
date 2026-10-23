export * as PluginInstall from "./plugin-install"

import path from "path"
import { existsSync } from "fs"

const RELEASED_SEMVER = /^\d+\.\d+\.\d+$/

/**
 * Pick the npm version specifier for `@opencode-ai/plugin`.
 *
 * Released builds pin the exact published semver. Every other channel (local, dev,
 * or a pre-release like `0.0.0-v1-<ts>`) has no artifact published under that
 * version, so pinning it makes `reify` fail; float to the `latest` dist-tag instead.
 */
export function pluginInstallVersion(version: string): string {
  return RELEASED_SEMVER.test(version) ? version : "@latest"
}

/**
 * Check that `@opencode-ai/plugin` actually landed in `dir`.
 *
 * `Npm.install` swallows read-only directories and can settle without a reify, so the
 * on-disk result is the only trustworthy signal that the config-dir guarantee held.
 * Returns a reason instead of throwing so callers can report loudly without failing.
 */
export function verifyPluginDeps(dir: string): { ok: boolean; reason?: string } {
  const nodeModules = path.join(dir, "node_modules")
  if (!existsSync(nodeModules)) return { ok: false, reason: `node_modules is missing at ${nodeModules}` }

  const pluginDir = path.join(nodeModules, "@opencode-ai", "plugin")
  if (!existsSync(pluginDir)) return { ok: false, reason: `@opencode-ai/plugin is missing at ${pluginDir}` }

  return { ok: true }
}
