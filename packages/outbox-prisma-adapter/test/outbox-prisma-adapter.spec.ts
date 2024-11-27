import { InMemoryOutboxAccumulator, type OutboxEntry } from '@message-queue-toolkit/outbox-core'
import {
  type CommonEventDefinition,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/schemas'
import { PrismaClient } from '@prisma/client'
import { uuidv7 } from 'uuidv7'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { z } from 'zod'
import { OutboxPrismaAdapter } from '../lib/outbox-prisma-adapter'

const events = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        message: z.string(),
      }),
    ),
  },
} satisfies Record<string, CommonEventDefinition>

type SupportedEvents = (typeof events)[keyof typeof events][]

describe('outbox-prisma-adapter', () => {
  let prisma: PrismaClient
  let outboxPrismaAdapter: OutboxPrismaAdapter<SupportedEvents>

  const ENTRY_1 = {
    id: uuidv7(),
    event: events.created,
    status: 'CREATED',
    data: {
      id: uuidv7(),
      payload: {
        message: 'TEST EVENT',
      },
      metadata: {},
      timestamp: new Date().toISOString(),
    },
    retryCount: 0,
    created: new Date(),
  } satisfies OutboxEntry<SupportedEvents[number]>

  const ENTRY_2 = {
    id: uuidv7(),
    event: events.created,
    status: 'CREATED',
    data: {
      id: uuidv7(),
      payload: {
        message: 'TEST EVENT 2',
      },
      metadata: {},
      timestamp: new Date().toISOString(),
    },
    retryCount: 0,
    created: new Date(),
  } satisfies OutboxEntry<SupportedEvents[number]>

  beforeAll(async () => {
    prisma = new PrismaClient({
      log: ['query'],
    })

    outboxPrismaAdapter = new OutboxPrismaAdapter<SupportedEvents>(prisma, 'OutboxEntry')

    await prisma.$queryRaw`create schema if not exists prisma;`
    await prisma.$queryRaw`
      CREATE TABLE prisma.outbox_entry (
        id UUID PRIMARY KEY, 
        type TEXT NOT NULL,
        created TIMESTAMP NOT NULL,
        updated TIMESTAMP,
        retry_count INT NOT NULL DEFAULT 0,
        data JSONB NOT NULL,
        status TEXT NOT NULL
      )
    `
  })

  beforeEach(async () => {
    await prisma.$queryRaw`DELETE FROM prisma.outbox_entry;`
  })

  afterAll(async () => {
    await prisma.$queryRaw`DROP TABLE prisma.outbox_entry;`
    await prisma.$queryRaw`DROP SCHEMA prisma;`
    await prisma.$disconnect()
  })

  it('creates entry in DB via outbox storage implementation', async () => {
    await outboxPrismaAdapter.createEntry({
      id: uuidv7(),
      event: events.created,
      status: 'CREATED',
      data: {
        id: uuidv7(),
        payload: {
          message: 'TEST EVENT',
        },
        metadata: {},
        timestamp: new Date().toISOString(),
      },
      retryCount: 0,
      created: new Date(),
    } satisfies OutboxEntry<SupportedEvents[number]>)

    const entries = await outboxPrismaAdapter.getEntries(10)

    expect(entries).toEqual([
      {
        id: expect.any(String),
        type: 'entity.created',
        created: expect.any(Date),
        updated: expect.any(Date),
        retryCount: 0,
        data: {
          id: expect.any(String),
          payload: {
            message: 'TEST EVENT',
          },
          metadata: {},
          timestamp: expect.any(String),
        },
        status: 'CREATED',
      },
    ])
  })

  it('should insert successful entries from accumulator', async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()
    accumulator.add(ENTRY_1)
    accumulator.add(ENTRY_2)

    await outboxPrismaAdapter.flush(accumulator)

    const entriesAfterFlush = await outboxPrismaAdapter.getEntries(10)

    expect(entriesAfterFlush).toMatchObject([
      {
        id: ENTRY_1.id,
        status: 'SUCCESS',
      },
      {
        id: ENTRY_2.id,
        status: 'SUCCESS',
      },
    ])
  })

  it("should update existing entries' status to 'SUCCESS'", async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()
    accumulator.add(ENTRY_1)
    accumulator.add(ENTRY_2)

    await outboxPrismaAdapter.createEntry(ENTRY_1)
    await outboxPrismaAdapter.createEntry(ENTRY_2)

    const beforeFlush = await outboxPrismaAdapter.getEntries(10)
    expect(beforeFlush).toMatchObject([
      {
        id: ENTRY_1.id,
        status: 'CREATED',
      },
      {
        id: ENTRY_2.id,
        status: 'CREATED',
      },
    ])

    await outboxPrismaAdapter.flush(accumulator)

    const afterFlush = await outboxPrismaAdapter.getEntries(10)
    expect(afterFlush).toMatchObject([
      {
        id: ENTRY_1.id,
        status: 'SUCCESS',
      },
      {
        id: ENTRY_2.id,
        status: 'SUCCESS',
      },
    ])
  })

  it('should handle mix of entries, non existing and existing, and change their status to SUCCESS', async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()
    accumulator.add(ENTRY_1)
    accumulator.add(ENTRY_2)

    //Only one exists in DB.
    await outboxPrismaAdapter.createEntry(ENTRY_2)

    await outboxPrismaAdapter.flush(accumulator)

    const afterFirstFlush = await outboxPrismaAdapter.getEntries(10)
    expect(afterFirstFlush).toMatchObject([
      {
        id: ENTRY_1.id,
        status: 'SUCCESS',
      },
      {
        id: ENTRY_2.id,
        status: 'SUCCESS',
      },
    ])
  })

  it("should change failed entries' status to 'FAILED' and increment retry count", async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()
    accumulator.addFailure(ENTRY_1)

    await outboxPrismaAdapter.flush(accumulator)

    const afterFirstFlush = await outboxPrismaAdapter.getEntries(10)
    expect(afterFirstFlush).toMatchObject([
      {
        id: ENTRY_1.id,
        status: 'FAILED',
        retryCount: 1,
      },
    ])
  })

  it('should change failed EXISTING entries status to FAILED and increment retry count', async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()
    const failedEntry = { ...ENTRY_1, retryCount: 3, status: 'FAILED' } satisfies OutboxEntry<
      SupportedEvents[number]
    >
    accumulator.addFailure(failedEntry)

    await outboxPrismaAdapter.createEntry(failedEntry)
    await outboxPrismaAdapter.flush(accumulator)

    const afterFirstFlush = await outboxPrismaAdapter.getEntries(10)
    expect(afterFirstFlush).toMatchObject([
      {
        id: failedEntry.id,
        status: 'FAILED',
        retryCount: 4,
      },
    ])
  })

  it('should not fetch entries that exceed retry limit', async () => {
    const failedEntry = { ...ENTRY_1, retryCount: 6, status: 'FAILED' } satisfies OutboxEntry<
      SupportedEvents[number]
    >
    await outboxPrismaAdapter.createEntry(failedEntry)

    const entries = await outboxPrismaAdapter.getEntries(5)

    expect(entries).toEqual([])
  })
})
