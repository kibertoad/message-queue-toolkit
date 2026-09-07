import { z } from 'zod/v4'
import { enrichEventSchemaWithBase } from './baseEventSchemas.ts'

const UNION_PAYLOAD_SCHEMA = z.union([
  z.object({ projectId: z.string(), value: z.string() }),
  z.object({ projectId: z.string(), values: z.array(z.string()) }),
])

const baseMessage = (type: string, payload: Record<string, unknown>) => ({
  id: '1',
  timestamp: new Date().toISOString(),
  type,
  payload,
  metadata: {
    schemaVersion: '1',
    producedBy: 'USER_SERVICE',
    originatedFrom: 'USER_SERVICE',
    correlationId: 'c1',
  },
})

describe('enrichEventSchemaWithBase', () => {
  it('accepts a plain object payload schema', () => {
    const schema = enrichEventSchemaWithBase('event.updated', z.object({ a: z.string() }))

    const parsed = schema.consumerSchema.parse(baseMessage('event.updated', { a: 'x' }))
    expect(parsed.payload).toEqual({ a: 'x' })
  })

  it('accepts a union payload schema', () => {
    const schema = enrichEventSchemaWithBase('event.union', UNION_PAYLOAD_SCHEMA)

    const parsed = schema.consumerSchema.parse(
      baseMessage('event.union', { projectId: 'p1', values: ['a', 'b'] }),
    )
    expect(parsed.payload).toEqual({ projectId: 'p1', values: ['a', 'b'] })
  })

  it('rejects a non-object payload schema', () => {
    // @ts-expect-error payload must be an object or a union of objects, not a bare scalar
    enrichEventSchemaWithBase('event.scalar', z.string())
  })

  it('rejects an `any` payload schema', () => {
    // @ts-expect-error z.any() would satisfy the object constraint yet disable payload checking
    enrichEventSchemaWithBase('event.any', z.any())
  })
})
