import { InMemoryOutboxAccumulator, type OutboxEntry } from '@message-queue-toolkit/outbox-core'
import {
  type CommonEventDefinition,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/schemas'
import { PrismaClient } from '@prisma/client'
import { uuidv7 } from 'uuidv7'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
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

  beforeAll(async () => {
    prisma = new PrismaClient()

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

    const entry1 = {
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
    accumulator.add(entry1)

    const entry2 = {
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
    accumulator.add(entry2)

    await outboxPrismaAdapter.flush(accumulator)

    const entriesAfterFlush = await outboxPrismaAdapter.getEntries(10)

    expect(entriesAfterFlush).toMatchObject([
      {
        id: entry1.id,
        status: 'SUCCESS',
      },
      {
        id: entry2.id,
        status: 'SUCCESS',
      },
    ])
  })

  it("should update successful entries' status to 'SUCCESS'", async () => {
    const accumulator = new InMemoryOutboxAccumulator<SupportedEvents>()

    const entry1 = {
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
    accumulator.add(entry1)

    const entry2 = {
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
    accumulator.add(entry2)

    await outboxPrismaAdapter.createEntry(entry1)
    await outboxPrismaAdapter.createEntry(entry2)

    const beforeFlush = await outboxPrismaAdapter.getEntries(10)
    expect(beforeFlush).toMatchObject([
      {
        id: entry1.id,
        status: 'CREATED',
      },
      {
        id: entry2.id,
        status: 'CREATED',
      },
    ])

    outboxPrismaAdapter.flush(accumulator)

    const afterFlush = await outboxPrismaAdapter.getEntries(10)
    expect(afterFlush).toMatchObject([
      {
        id: entry1.id,
        status: 'SUCCESS',
      },
      {
        id: entry2.id,
        status: 'SUCCESS',
      },
    ])
  })
})
