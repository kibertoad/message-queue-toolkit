import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { OutboxAccumulator } from './accumulators.ts'
import type { OutboxEntry } from './objects.ts'

/**
 * Takes care of persisting and retrieving outbox entries.
 *
 * Implementation is required:
 * - in order to fulfill at least once delivery guarantee, persisting entries should be performed inside isolated transaction
 * - to return entries in the order they were created (UUID7 is used to create entries in OutboxEventEmitter)
 * - returned entries should not include the ones with 'SUCCESS' status
 */
export interface OutboxStorage<SupportedEvents extends CommonEventDefinition[]> {
  createEntry(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>>

  /**
   * Responsible for taking all entries from the accumulator and persisting them in the storage.
   *
   * - Items that are in OutboxAccumulator::getEntries MUST be changed to SUCCESS status and `updatedAt` field needs to be set.
   * - Items that are in OutboxAccumulator::getFailedEntries MUST be changed to FAILED status, `updatedAt` field needs to be set and retryCount needs to be incremented.
   */
  flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void>

  /**
   * Returns entries in the order they were created. It doesn't return entries with 'SUCCESS' status. It doesn't return entries that have been retried more than maxRetryCount times.
   *
   * For example if entry retryCount is 1 and maxRetryCount is 1, entry MUST be returned. If it fails again then retry count is 2, in that case entry MUST NOT be returned.
   */
  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]>
}
