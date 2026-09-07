import { expectTypeOf } from 'vitest'
import { z } from 'zod/v4'
import type { SnsAwareEventDefinition } from '../vendors/snsSchemas.ts'
import { enrichMessageSchemaWithBase } from './baseMessageSchemas.ts'

const UNION_PAYLOAD_SCHEMA = z.union([
  z.object({ projectId: z.string(), value: z.string() }),
  z.object({ projectId: z.string(), values: z.array(z.string()) }),
])

const myEvents = {
  myEvent: {
    ...enrichMessageSchemaWithBase('user.updated', z.object({})),
    snsTopic: 'user',
    producedBy: ['USER_SERVICE'],
  },
  myUnionEvent: {
    ...enrichMessageSchemaWithBase('user.union_updated', UNION_PAYLOAD_SCHEMA),
    snsTopic: 'user',
    producedBy: ['USER_SERVICE'],
  },
} as const satisfies Record<string, SnsAwareEventDefinition>

describe('base message schemas', () => {
  it('message satisfies SnsAwareEventDefinition', () => {
    expectTypeOf(myEvents.myEvent).toExtend<SnsAwareEventDefinition>()
  })

  it('accepts a payload schema that transforms one object into another', () => {
    const schema = enrichMessageSchemaWithBase(
      'user.updated',
      z.object({ a: z.string() }).transform((v) => ({ ...v, extra: 1 })),
    )

    const parsed = schema.consumerSchema.parse({
      id: '1',
      timestamp: new Date().toISOString(),
      type: 'user.updated',
      payload: { a: 'x' },
      metadata: {
        schemaVersion: '1',
        producedBy: 'USER_SERVICE',
        originatedFrom: 'USER_SERVICE',
        correlationId: 'c1',
      },
    })
    expect(parsed.payload).toEqual({ a: 'x', extra: 1 })
  })

  it('accepts a union payload schema', () => {
    const parsed = myEvents.myUnionEvent.consumerSchema.parse({
      id: '1',
      timestamp: new Date().toISOString(),
      type: 'user.union_updated',
      payload: { projectId: 'p1', values: ['a', 'b'] },
      metadata: {
        schemaVersion: '1',
        producedBy: 'USER_SERVICE',
        originatedFrom: 'USER_SERVICE',
        correlationId: 'c1',
      },
    })
    expect(parsed.payload).toEqual({ projectId: 'p1', values: ['a', 'b'] })
  })
})
