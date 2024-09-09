import { PrismaClient } from '@prisma/client'
import { beforeAll, describe, it } from 'vitest'

describe('outbox-prisma-adapter', () => {
  let prisma: PrismaClient

  beforeAll(async () => {
    prisma = new PrismaClient()

    await prisma.$queryRaw`
      CREATE TABLE prisma.outbox_entry (id UUID PRIMARY KEY, created TIMESTAMP NOT NULL)`
  })

  it('created outbox entry', async () => {
    const result = await prisma.$queryRaw`SELECT 1 as counter;`

    console.log(result)

    await prisma.outboxEntry.create({
      data: {
        id: 'ce08b43b-6162-4913-86ea-fa9367875e3b',
        created: new Date(),
      },
    })
  })
})
