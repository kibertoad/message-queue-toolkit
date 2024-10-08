import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { OutboxEntry } from './objects.ts'

/**
 * Accumulator is responsible for storing outbox entries in two cases:
 * - successfully dispatched event
 * - failed events
 *
 * Thanks to this, we can use aggregated result and persist in the storage in batches.
 */
export interface OutboxAccumulator<SupportedEvents extends CommonEventDefinition[]> {
  /**
   * Accumulates successfully dispatched event.
   * @param outboxEntry
   */
  add(outboxEntry: OutboxEntry<SupportedEvents[number]>): Promise<void>

  /**
   * Accumulates failed event.
   * @param outboxEntry
   */
  addFailure(outboxEntry: OutboxEntry<SupportedEvents[number]>): Promise<void>

  /**
   * Returns all entries that should be persisted as successful ones.
   */
  getEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]>

  /**
   * Returns all entries that should be persisted as failed ones. Such entries will be retried + their retryCount will be incremented.
   */
  getFailedEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]>

  /**
   * After running clear(), no entries should be returned by getEntries() and getFailedEntries().
   *
   * clear() is always called after flush() in OutboxStorage.
   */
  clear(): Promise<void>
}

export class InMemoryOutboxAccumulator<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxAccumulator<SupportedEvents>
{
  private entries: OutboxEntry<SupportedEvents[number]>[] = []
  private failedEntries: OutboxEntry<SupportedEvents[number]>[] = []

  public add(outboxEntry: OutboxEntry<SupportedEvents[number]>) {
    this.entries.push(outboxEntry)

    return Promise.resolve()
  }

  public addFailure(outboxEntry: OutboxEntry<SupportedEvents[number]>) {
    this.failedEntries.push(outboxEntry)

    return Promise.resolve()
  }

  getEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    return Promise.resolve(this.entries)
  }

  getFailedEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    return Promise.resolve(this.failedEntries)
  }

  public clear(): Promise<void> {
    this.entries = []
    this.failedEntries = []
    return Promise.resolve()
  }
}
