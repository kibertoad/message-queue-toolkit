import type { OutboxEntry } from '@message-queue-toolkit/outbox-core'
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
        created TIMESTAMP NOT NULL
      )
    `
  })

  afterAll(async () => {
    await prisma.$queryRaw`DROP TABLE prisma.outbox_entry;`
    await prisma.$queryRaw`DROP SCHEMA prisma;`
    await prisma.$disconnect()
  })

  it('test db connection', async () => {
    const creationDate = new Date()
    await prisma.outboxEntry.create({
      data: {
        id: 'ce08b43b-6162-4913-86ea-fa9367875e3b',
        created: creationDate,
      },
    })

    const result = await prisma.outboxEntry.findMany()
    expect(result).toEqual([
      {
        id: 'ce08b43b-6162-4913-86ea-fa9367875e3b',
        created: creationDate,
      },
    ])
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
    } satisfies OutboxEntry<SupportedEvents[number]>)
  })
})
