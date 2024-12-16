import type { TransactionObservabilityManager } from '@lokalise/node-core'

export class FakeTransactionObservabilityManager implements TransactionObservabilityManager {
  start() {}

  startWithGroup() {}

  stop() {}

  addCustomAttributes() {}
}
