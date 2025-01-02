import type { TransactionObservabilityManager } from '@lokalise/node-core'

export class FakeTransactionObservabilityManager implements TransactionObservabilityManager {
  addCustomAttributes(): void {}

  start() {}

  startWithGroup() {}

  stop() {}
}
