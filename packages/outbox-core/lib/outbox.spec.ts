import { randomUUID } from 'node:crypto'
import {
  CommonMetadataFiller,
  DomainEventEmitter,
  EventRegistry,
} from '@message-queue-toolkit/core'
import {
  type CommonEventDefinition,
  type CommonEventDefinitionPublisherSchemaType,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/schemas'
import pino, { type Logger } from 'pino'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { z } from 'zod'
import { InMemoryOutboxAccumulator, type OutboxAccumulator } from './accumulators'
import type { OutboxEntry } from './objects'
import { type OutboxDependencies, OutboxEventEmitter, OutboxProcessor } from './outbox'
import type { OutboxStorage } from './storage'

const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        message: z.string(),
      }),
    ),
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        message: z.string(),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]

const createdEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.created> = {
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
  metadata: {
    originatedFrom: 'service',
    producedBy: 'producer',
    schemaVersion: '1',
    correlationId: randomUUID(),
  },
}

const TestLogger: Logger = pino()

class InMemoryOutboxStorage<SupportedEvents extends CommonEventDefinition[]>
  implements OutboxStorage<SupportedEvents>
{
  public entries: OutboxEntry<SupportedEvents[number]>[] = []

  create(
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

const MAX_RETRY_COUNT = 2

describe('outbox', () => {
  let outboxProcessor: OutboxProcessor<TestEventsType>
  let eventEmitter: DomainEventEmitter<TestEventsType>
  let outboxEventEmitter: OutboxEventEmitter<TestEventsType>
  let outboxStorage: InMemoryOutboxStorage<TestEventsType>

  beforeEach(() => {
    eventEmitter = new DomainEventEmitter({
      logger: TestLogger,
      errorReporter: { report: () => {} },
      eventRegistry: new EventRegistry(Object.values(TestEvents)),
      metadataFiller: new CommonMetadataFiller({
        serviceId: 'test',
      }),
    })

    outboxStorage = new InMemoryOutboxStorage<TestEventsType>()
    outboxEventEmitter = new OutboxEventEmitter<TestEventsType>(outboxStorage)
    outboxProcessor = new OutboxProcessor<TestEventsType>(
      {
        outboxStorage,
        //@ts-ignore
        outboxAccumulator: new InMemoryOutboxAccumulator(),
        eventEmitter,
      } satisfies OutboxDependencies<TestEventsType>,
      MAX_RETRY_COUNT,
      1,
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('saves outbox entry to storage', async () => {
    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    const entries = await outboxStorage.getEntries(MAX_RETRY_COUNT)

    expect(entries).toHaveLength(1)
  })

  it('saves outbox entry and process it', async () => {
    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })

    const entries = await outboxStorage.getEntries(MAX_RETRY_COUNT)

    expect(entries).toHaveLength(0)
    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'SUCCESS',
      },
    ])
  })

  it('saves outbox entry and process it with error and retries', async () => {
    const mockedEventEmitter = vi.spyOn(eventEmitter, 'emit')
    mockedEventEmitter.mockImplementationOnce(() => {
      throw new Error('Could not emit event.')
    })
    mockedEventEmitter.mockImplementationOnce(() =>
      Promise.resolve({
        ...createdEventPayload,
        id: randomUUID(),
        timestamp: new Date().toISOString(),
        metadata: {
          schemaVersion: '1',
          producedBy: 'test',
          originatedFrom: 'service',
          correlationId: randomUUID(),
        },
      }),
    )

    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })

    let entries = await outboxStorage.getEntries(MAX_RETRY_COUNT)
    expect(entries).toHaveLength(1)
    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'FAILED',
        retryCount: 1,
      },
    ])

    //Now let's process again successfully
    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })

    entries = await outboxStorage.getEntries(MAX_RETRY_COUNT)
    expect(entries).toHaveLength(0) //Nothing to process anymore
    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'SUCCESS',
        retryCount: 1,
      },
    ])
  })

  it('no longer processes the event if exceeded retry count', async () => {
    //Let's always fail the event
    const mockedEventEmitter = vi.spyOn(eventEmitter, 'emit')
    mockedEventEmitter.mockImplementation(() => {
      throw new Error('Could not emit event.')
    })

    //Persist the event
    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    //Initially event is present in outbox storage.
    expect(await outboxStorage.getEntries(MAX_RETRY_COUNT)).toHaveLength(1)

    //Retry +1
    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })
    //Still present
    expect(await outboxStorage.getEntries(MAX_RETRY_COUNT)).toHaveLength(1)

    //Retry +2
    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })
    //Stil present
    expect(await outboxStorage.getEntries(MAX_RETRY_COUNT)).toHaveLength(1)

    //Retry +3
    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })
    //Now it's gone, we no longer try to process it
    expect(await outboxStorage.getEntries(MAX_RETRY_COUNT)).toHaveLength(0)

    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'FAILED',
        retryCount: 3,
      },
    ])
  })
})
