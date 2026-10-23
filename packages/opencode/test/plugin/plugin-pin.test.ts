import { describe, expect, test } from "bun:test"
import { pluginInstallVersion } from "@/config/plugin-install"

describe("plugin.install.pin", () => {
  test("keeps a released semver version", () => {
    expect(pluginInstallVersion("1.19.4")).toBe("1.19.4")
  })

  test("floats (undefined) for an unpublished dev version", () => {
    expect(pluginInstallVersion("0.0.0-v1-202610230411")).toBeUndefined()
  })

  test("floats (undefined) for the local sentinel", () => {
    expect(pluginInstallVersion("local")).toBeUndefined()
  })
})
