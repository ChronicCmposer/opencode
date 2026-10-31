import { LazyJson } from "./lazy-json"

const nonce = crypto.randomUUID()

const resolve = (root: unknown, path: ReadonlyArray<string>): LazyJson | undefined => {
  let node = root
  for (const key of path) {
    if (typeof node !== "object" || node === null) return undefined
    node = (node as Record<string, unknown>)[key]
  }
  return LazyJson.is(node) ? node : undefined
}

const replace = (root: unknown, path: ReadonlyArray<string>, token: string): unknown => {
  if (path.length === 0) return token
  const [key, ...rest] = path
  const node = root as Record<string, unknown>
  return { ...node, [key]: replace(node[key], rest, token) }
}

/**
 * JSON.stringify that splices the raw text of a `LazyJson` found at any of `paths` instead of parsing and
 * re-stringifying it. Output parses to exactly what plain `JSON.stringify` produces (`LazyJson.toJSON` yields its value).
 */
export function stringifySpliced(value: unknown, paths: ReadonlyArray<ReadonlyArray<string>>): string {
  const found: Array<{ token: string; lazy: LazyJson }> = []
  let shaped = value
  for (const path of paths) {
    const lazy = resolve(shaped, path)
    if (!lazy) continue
    const token = `${nonce}:${found.length}`
    found.push({ token, lazy })
    shaped = replace(shaped, path, token)
  }
  const out = JSON.stringify(shaped)
  if (found.length === 0) return out
  let result = out
  for (const { token, lazy } of found) {
    const quoted = `"${token}"`
    const at = result.indexOf(quoted)
    result = result.slice(0, at) + lazy.text + result.slice(at + quoted.length)
  }
  return result
}
