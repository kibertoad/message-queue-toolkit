import type { OutboxAccumulator, OutboxEntry } from '@message-queue-toolkit/outbox-core'
import type { OutboxStorage } from '@message-queue-toolkit/outbox-core/dist/lib/storage'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'

export class OutboxPrismaAdapter<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxStorage<SupportedEvents>
{
  createEntry(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>> {
    return Promise.resolve(undefined)
  }

  flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void> {
    return Promise.resolve(undefined)
  }

  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    return Promise.resolve([])
  }
}
