import type {
  OutboxAccumulator,
  OutboxEntry,
  OutboxStorage,
} from '@message-queue-toolkit/outbox-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { Prisma, PrismaClient } from '@prisma/client'

export type EnrichedOutboxEntry<Event extends CommonEventDefinition> = OutboxEntry<Event> & {
  type: string
}

type ModelDelegate<Event extends CommonEventDefinition> = {
  create: (args: { data: EnrichedOutboxEntry<Event> }) => Promise<EnrichedOutboxEntry<Event>>
  findMany: (args: {
    where:
      | Partial<EnrichedOutboxEntry<Event>>
      | {
          id?: Prisma.StringFilter
          retryCount?: Prisma.IntFilter
        }
  }) => Promise<EnrichedOutboxEntry<Event>[]>
  createMany: (args: { data: EnrichedOutboxEntry<Event>[] }) => Promise<void>
  updateMany: (args: {
    where: {
      id: {
        in: string[]
      }
    }
    data:
      | Partial<OutboxEntry<Event>>
      | {
          retryCount?: number | Prisma.IntFieldUpdateOperationsInput
        }
  }) => Promise<void>
}

export class OutboxPrismaAdapter<
  SupportedEvents extends CommonEventDefinition[],
  ModelName extends keyof PrismaClient & string,
> implements OutboxStorage<SupportedEvents>
{
  constructor(
    private readonly prisma: PrismaClient,
    private readonly modelName: ModelName,
  ) {}

  createEntry(
    outboxEntry: OutboxEntry<SupportedEvents[number]>,
  ): Promise<OutboxEntry<SupportedEvents[number]>> {
    const prismaModel = this.prisma[this.modelName] as unknown as ModelDelegate<
      SupportedEvents[number]
    >

    return prismaModel.create({
      data: {
        id: outboxEntry.id,
        type: outboxEntry.event.type,
        created: outboxEntry.created,
        updated: outboxEntry.updated,
        event: outboxEntry.event,
        status: outboxEntry.status,
        retryCount: outboxEntry.retryCount,
      },
    })
  }

  async flush(outboxAccumulator: OutboxAccumulator<SupportedEvents>): Promise<void> {
    const entries = await outboxAccumulator.getEntries()
    const failedEntries = await outboxAccumulator.getFailedEntries()
    const prismaModel = this.prisma[this.modelName] as unknown as ModelDelegate<
      SupportedEvents[number]
    >

    const existingEntries = await prismaModel.findMany({
      where: {
        id: {
          in: [...entries.map((entry) => entry.id), ...failedEntries.map((entry) => entry.id)],
        },
      },
    })

    await this.prisma.$transaction(async (prisma) => {
      const prismaModel = prisma[this.modelName] as unknown as ModelDelegate<
        SupportedEvents[number]
      >
      await this.handleSuccesses(prismaModel, entries, existingEntries)
      await this.handleFailures(prismaModel, failedEntries, existingEntries)
    })
  }

  private async handleSuccesses(
    prismaModel: ModelDelegate<SupportedEvents[number]>,
    entries: OutboxEntry<SupportedEvents[number]>[],
    existingEntries: OutboxEntry<SupportedEvents[number]>[],
  ) {
    const toCreate = entries.filter(
      (entry) => !existingEntries.some((existingEntry) => existingEntry.id === entry.id),
    )
    const toUpdate = entries.filter((entry) =>
      existingEntries.some((existingEntry) => existingEntry.id === entry.id),
    )

    if (toCreate.length > 0) {
      await prismaModel.createMany({
        data: toCreate.map((entry) => ({
          id: entry.id,
          type: entry.event.type,
          created: entry.created,
          updated: new Date(),
          event: entry.event,
          status: 'SUCCESS',
          retryCount: entry.retryCount,
        })),
      })
    }

    if (toUpdate.length > 0) {
      await prismaModel.updateMany({
        where: {
          id: {
            in: toUpdate.map((entry) => entry.id),
          },
        },
        data: {
          status: 'SUCCESS',
          updated: new Date(),
        },
      })
    }
  }

  private async handleFailures(
    prismaModel: ModelDelegate<SupportedEvents[number]>,
    entries: OutboxEntry<SupportedEvents[number]>[],
    existingEntries: OutboxEntry<SupportedEvents[number]>[],
  ) {
    const toCreate = entries.filter(
      (entry) => !existingEntries.some((existingEntry) => existingEntry.id === entry.id),
    )
    const toUpdate = entries.filter((entry) =>
      existingEntries.some((existingEntry) => existingEntry.id === entry.id),
    )

    if (toCreate.length > 0) {
      await prismaModel.createMany({
        data: toCreate.map((entry) => ({
          id: entry.id,
          type: entry.event.type,
          created: entry.created,
          updated: new Date(),
          event: entry.event,
          status: 'FAILED',
          retryCount: 1,
        })),
      })
    }

    if (toUpdate.length > 0) {
      await prismaModel.updateMany({
        where: {
          id: {
            in: toUpdate.map((entry) => entry.id),
          },
        },
        data: {
          status: 'FAILED',
          updated: new Date(),
          retryCount: {
            increment: 1,
          },
        },
      })
    }
  }

  getEntries(maxRetryCount: number): Promise<OutboxEntry<SupportedEvents[number]>[]> {
    const prismaModel = this.prisma[this.modelName] as unknown as ModelDelegate<
      SupportedEvents[number]
    >

    return prismaModel.findMany({
      where: {
        retryCount: {
          lte: maxRetryCount,
        },
      },
    })
  }
}
