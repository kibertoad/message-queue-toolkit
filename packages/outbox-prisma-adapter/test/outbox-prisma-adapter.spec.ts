import { PrismaClient } from '@prisma/client'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

describe('outbox-prisma-adapter', () => {
  let prisma: PrismaClient

  beforeAll(async () => {
    prisma = new PrismaClient()

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
})
