import { ConfigScope } from '@lokalise/node-core'

const configScope = new ConfigScope()

export function reloadConfig() {
  configScope.updateEnv()
}

export function isProduction(): boolean {
  return configScope.isProduction()
}
