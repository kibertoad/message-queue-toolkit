import { expectTypeOf } from 'vitest'
import { z } from 'zod/v4'
import type { SnsAwareEventDefinition } from '../vendors/snsSchemas.ts'
import { enrichMessageSchemaWithBase } from './baseMessageSchemas.ts'

const myEvents = {
  myEvent: {
    ...enrichMessageSchemaWithBase('user.updated', z.object({})),
    snsTopic: 'user',
    producedBy: ['USER_SERVICE'],
  },
} as const satisfies Record<string, SnsAwareEventDefinition>

describe('base message schemas', () => {
  it('message satisfies SnsAwareEventDefinition', () => {
    expectTypeOf(myEvents.myEvent).toExtend<SnsAwareEventDefinition>()
  })
})
