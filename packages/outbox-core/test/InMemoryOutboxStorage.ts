import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { OutboxAccumulator } from '../lib/accumulators'
import type { OutboxEntry } from '../lib/objects'
import type { OutboxStorage } from '../lib/storage'

export class InMemoryOutboxStorage<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxStorage<SupportedEvents>
{
  public entries: OutboxEntry<SupportedEvents[number]>[] = []

  createEntry(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>> {
    this.entries = [...this.entries, outboxEntry]

    return Promise.resolve(outboxEntry)
  }

  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    const entries = this.entries.filter((entry) => {
      return entry.status !== 'SUCCESS' && entry.retryCount <= maxRetryCount
    })

    return Promise.resolve(entries)
  }

  update(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>> {
    this.entries = this.entries.map((entry) => {
      if (entry.id === outboxEntry.id) {
        return outboxEntry
      }
      return entry
    })

    return Promise.resolve(outboxEntry)
  }

  public async flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void> {
    let successEntries = await outboxAccumulator.getEntries()
    successEntries = successEntries.map((entry) => {
      return {
        ...entry,
        status: 'SUCCESS',
        updateAt: new Date(),
      }
    })
    this.entries = this.entries.map((entry) => {
      const foundEntry = successEntries.find((successEntry) => successEntry.id === entry.id)
      if (foundEntry) {
        return foundEntry
      }
      return entry
    })

    let failedEntries = await outboxAccumulator.getFailedEntries()
    failedEntries = failedEntries.map((entry) => {
      return {
        ...entry,
        status: 'FAILED',
        updateAt: new Date(),
        retryCount: entry.retryCount + 1,
      }
    })
    this.entries = this.entries.map((entry) => {
      const foundEntry = failedEntries.find((failedEntry) => failedEntry.id === entry.id)
      if (foundEntry) {
        return foundEntry
      }
      return entry
    })
  }
}
