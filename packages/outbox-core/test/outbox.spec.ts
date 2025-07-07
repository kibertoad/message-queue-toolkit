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
import { type Logger, pino } from 'pino'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { z } from 'zod/v4'
import { InMemoryOutboxAccumulator } from '../lib/accumulators.ts'
import { type OutboxDependencies, OutboxEventEmitter, OutboxProcessor } from '../lib/outbox.ts'
import { InMemoryOutboxStorage } from './InMemoryOutboxStorage.ts'

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

const MAX_RETRY_COUNT = 2

describe('outbox', () => {
  let outboxProcessor: OutboxProcessor<TestEventsType>
  let eventEmitter: DomainEventEmitter<TestEventsType>
  let outboxEventEmitter: OutboxEventEmitter<TestEventsType>
  let outboxStorage: InMemoryOutboxStorage<TestEventsType>
  let inMemoryOutboxAccumulator: InMemoryOutboxAccumulator<TestEventsType>

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
    inMemoryOutboxAccumulator = new InMemoryOutboxAccumulator()
    outboxProcessor = new OutboxProcessor<TestEventsType>(
      {
        outboxStorage,
        //@ts-ignore
        outboxAccumulator: inMemoryOutboxAccumulator,
        eventEmitter,
      } satisfies OutboxDependencies<TestEventsType>,
      { maxRetryCount: MAX_RETRY_COUNT, emitBatchSize: 1 },
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

  it("doesn't emit event again if it's already present in accumulator", async () => {
    const mockedEventEmitter = vi.spyOn(eventEmitter, 'emit')

    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    await inMemoryOutboxAccumulator.add(outboxStorage.entries[0]!)

    await outboxProcessor.processOutboxEntries({
      logger: TestLogger,
      reqId: randomUUID(),
      executorId: randomUUID(),
    })

    //We pretended that event was emitted in previous run by adding state to accumulator
    expect(mockedEventEmitter).toHaveBeenCalledTimes(0)

    //But after the loop, if successful, it should be marked as success anyway
    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'SUCCESS',
      },
    ])
    //And accumulator should be cleared
    expect(await inMemoryOutboxAccumulator.getEntries()).toHaveLength(0)
  })
})
