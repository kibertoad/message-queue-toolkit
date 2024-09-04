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
import { type OutboxEntry, OutboxEventEmitter, OutboxProcessor, type OutboxStorage } from './outbox'

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

const updatedEventPayload: CommonEventDefinitionPublisherSchemaType<typeof TestEvents.updated> = {
  ...createdEventPayload,
  type: 'entity.updated',
}

const expectedCreatedPayload = {
  id: expect.any(String),
  timestamp: expect.any(String),
  payload: {
    message: 'msg',
  },
  type: 'entity.created',
  metadata: {
    correlationId: expect.any(String),
    originatedFrom: 'service',
    producedBy: 'producer',
    schemaVersion: '1',
  },
}

const expectedUpdatedPayload = {
  ...expectedCreatedPayload,
  type: 'entity.updated',
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
}

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
    outboxProcessor = new OutboxProcessor<TestEventsType>(outboxStorage, eventEmitter, 2)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('saves outbox entry to storage', async () => {
    await outboxEventEmitter.emit(TestEvents.created, createdEventPayload, {
      correlationId: randomUUID(),
    })

    const entries = await outboxStorage.getEntries(2)

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

    const entries = await outboxStorage.getEntries(2)

    expect(entries).toHaveLength(0)

    expect(outboxStorage.entries).toMatchObject([
      {
        status: 'SUCCESS',
      },
    ])
  })
})
