export * as PluginInstall from "./plugin-install"

import path from "path"
import { existsSync, readFileSync } from "fs"

const RELEASED_SEMVER = /^\d+\.\d+\.\d+$/
const PLUGIN_PACKAGE = "@opencode-ai/plugin"

/**
 * Pick the npm version specifier for `@opencode-ai/plugin`.
 *
 * Released builds pin the exact published semver. Every other channel (local, dev,
 * or a pre-release like `0.0.0-v1-<ts>`) has no artifact published under that
 * version, so pinning it makes `reify` fail; return `undefined` to float to the
 * `latest` dist-tag instead. `undefined` is the natural "no specifier" signal, so
 * callers pass it straight through to `Npm.install`.
 */
export function pluginInstallVersion(version: string): string | undefined {
  return RELEASED_SEMVER.test(version) ? version : undefined
}

interface PluginManifest {
  readonly name?: unknown
  readonly version?: unknown
}

/**
 * Read and parse `@opencode-ai/plugin`'s manifest. Returns `undefined` when the
 * file is missing or unparseable, so the caller reports a single loud reason
 * rather than throwing out of a background install.
 */
function readPluginManifest(pluginDir: string): PluginManifest | undefined {
  const manifestPath = path.join(pluginDir, "package.json")
  if (!existsSync(manifestPath)) return undefined
  try {
    return JSON.parse(readFileSync(manifestPath, "utf8")) as PluginManifest
  } catch {
    return undefined
  }
}

/**
 * Check that `@opencode-ai/plugin` actually landed in `dir` with a resolvable
 * manifest.
 *
 * `Npm.install` swallows read-only directories and can settle without a reify, so
 * the on-disk result is the only trustworthy signal that the config-dir guarantee
 * held. A directory alone is not enough: an empty or partial `node_modules` entry
 * must fail loud, so we require a parseable `package.json` that declares the
 * expected name and a version. Returns a reason instead of throwing so callers can
 * report loudly without failing.
 */
export function verifyPluginDeps(dir: string): { ok: boolean; reason?: string } {
  const nodeModules = path.join(dir, "node_modules")
  if (!existsSync(nodeModules)) return { ok: false, reason: `node_modules is missing at ${nodeModules}` }

  const pluginDir = path.join(nodeModules, "@opencode-ai", "plugin")
  if (!existsSync(pluginDir)) return { ok: false, reason: `@opencode-ai/plugin is missing at ${pluginDir}` }

  const manifest = readPluginManifest(pluginDir)
  if (manifest === undefined) {
    return { ok: false, reason: `@opencode-ai/plugin has no readable package.json at ${pluginDir}` }
  }
  if (manifest.name !== PLUGIN_PACKAGE) {
    return { ok: false, reason: `@opencode-ai/plugin package.json declares name ${JSON.stringify(manifest.name)}` }
  }
  if (typeof manifest.version !== "string" || manifest.version.length === 0) {
    return { ok: false, reason: `@opencode-ai/plugin package.json declares no version at ${pluginDir}` }
  }

  return { ok: true }
}
