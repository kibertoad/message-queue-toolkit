import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { OutboxEntry } from './objects.ts'

export interface OutboxAccumulator<SupportedEvents extends CommonEventDefinition[]> {
  add(outboxEntry: OutboxEntry<SupportedEvents[number]>): Promise<void>

  addFailure(outboxEntry: OutboxEntry<SupportedEvents[number]>): Promise<void>

  getEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]>

  getFailedEntries(): Promise<OutboxEntry<SupportedEvents[number]>[]>

  clear(): Promise<void>
}

export class InMemoryOutboxAccumulator<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxAccumulator<SupportedEvents>
{
  private entries: OutboxEntry<SupportedEvents[number]>[] = []
  private failedEntries: OutboxEntry<SupportedEvents[number]>[] = []

  public add(outboxEntry: OutboxEntry<SupportedEvents[number]>) {
    this.entries = [...this.entries, outboxEntry]

    return Promise.resolve()
  }

  public addFailure(outboxEntry: OutboxEntry<SupportedEvents[number]>) {
    this.failedEntries = [...this.failedEntries, outboxEntry]

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
