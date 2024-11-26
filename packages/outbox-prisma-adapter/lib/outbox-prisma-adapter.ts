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

    const messageType = getMessageType(outboxEntry.event)
    return prismaModel.create({
      data: {
        id: outboxEntry.id,
        type: messageType,
        created: outboxEntry.created,
        updated: outboxEntry.updated,
        data: outboxEntry.data,
        status: outboxEntry.status,
      },
    })
  }

  async flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void> {
    const entries = await outboxAccumulator.getEntries()

    const prismaModel: PrismaClient[typeof this.modelName] = this.prisma[this.modelName]

    for (const entry of entries) {
      await prismaModel.upsert({
        where: {
          id: entry.id,
        },
        update: {
          status: 'SUCCESS',
          updated: new Date(),
        },
        create: {
          id: entry.id,
          type: getMessageType(entry.event),
          created: entry.created,
          updated: new Date(),
          data: entry.data,
          status: 'SUCCESS',
        },
      })
    }
  }

  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    return this.prisma[this.modelName].findMany({
      where: {
        retryCount: {
          lte: maxRetryCount,
        },
      },
    })
  }
}
