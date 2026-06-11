import {
  type CommonEventDefinition,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/schemas'
import { describe, expectTypeOf, it } from 'vitest'
import { z } from 'zod/v4'

import type { DomainEventEmitter } from './DomainEventEmitter.ts'

const myEvents = {
  transformingEvent: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        mode: z.preprocess(
          (value) => (value === 'live' ? value : undefined),
          z.literal('live').optional(),
        ),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

type Emitter = DomainEventEmitter<[typeof myEvents.transformingEvent]>

describe('DomainEventEmitter types', () => {
  it('on() handlers receive the parsed event, with transformed fields as their output type', () => {
    type OnHandler = Parameters<Emitter['on']>[1]
    type HandledEvent = Parameters<OnHandler['handleEvent']>[0]

    expectTypeOf<HandledEvent['payload']['mode']>().toEqualTypeOf<'live' | undefined>()
  })

  it('onAny() handlers receive the parsed event, with transformed fields as their output type', () => {
    type AnyHandler = Parameters<Emitter['onAny']>[0]
    type HandledEvent = Parameters<AnyHandler['handleEvent']>[0]

    expectTypeOf<HandledEvent['payload']['mode']>().toEqualTypeOf<'live' | undefined>()
  })

  it('emit() takes the raw event as input and resolves to the parsed event', () => {
    type EmitInput = Parameters<Emitter['emit']>[1]
    type EmittedEvent = Awaited<ReturnType<Emitter['emit']>>

    // The caller passes the raw payload, which emit() parses with the schema
    expectTypeOf<EmitInput['payload']['mode']>().toBeUnknown()
    // What comes back is the validated event returned by the parse
    expectTypeOf<EmittedEvent['payload']['mode']>().toEqualTypeOf<'live' | undefined>()
  })
})
