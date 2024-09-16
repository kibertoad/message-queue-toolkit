import type { OutboxAccumulator, OutboxEntry } from '@message-queue-toolkit/outbox-core'
import type { OutboxStorage } from '@message-queue-toolkit/outbox-core/dist/lib/storage'
import { type CommonEventDefinition, getMessageType } from '@message-queue-toolkit/schemas'
import type { PrismaClient } from '@prisma/client'

export class OutboxPrismaAdapter<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxStorage<SupportedEvents>
{
  constructor(
    private readonly prisma: PrismaClient,
    private readonly modelName: string,
  ) {}

  createEntry(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>> {
    const prismaModel: PrismaClient[typeof this.modelName] = this.prisma[this.modelName]

    return prismaModel.create({
      data: getMessageType(outboxEntry.event),
    })
  }

  flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void> {
    return Promise.resolve(undefined)
  }

  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    return Promise.resolve([])
  }
}
