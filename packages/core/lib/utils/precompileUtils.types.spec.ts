import { describe, expectTypeOf, it } from 'vitest'
import { z } from 'zod/v4'
import { MessageHandlerConfig, MessageHandlerConfigBuilder } from '../queues/HandlerContainer.ts'
import type { QueuePublisherOptions } from '../types/queueOptionsTypes.ts'
import type { PrecompiledSchema } from './precompileUtils.ts'
import { precompileSchema } from './precompileUtils.ts'

const MESSAGE_SCHEMA = z.object({
  type: z.literal('message.a'),
  payload: z.object({ name: z.string() }),
})
type Message = z.output<typeof MESSAGE_SCHEMA>

const PRECOMPILED_MESSAGE_SCHEMA = precompileSchema(MESSAGE_SCHEMA)

const noop = () => Promise.resolve({ result: 'success' as const })

describe('precompileSchema types', () => {
  it('keeps the input and output types of the schema it was built from', () => {
    expectTypeOf<z.output<typeof PRECOMPILED_MESSAGE_SCHEMA>>().toEqualTypeOf<Message>()
    expectTypeOf<z.input<typeof PRECOMPILED_MESSAGE_SCHEMA>>().toEqualTypeOf<
      z.input<typeof MESSAGE_SCHEMA>
    >()
  })

  it('marks the result as precompiled', () => {
    expectTypeOf(PRECOMPILED_MESSAGE_SCHEMA).toExtend<PrecompiledSchema<unknown>>()
    expectTypeOf(MESSAGE_SCHEMA).not.toExtend<PrecompiledSchema<unknown>>()
  })
})

describe('rejecting precompiled schemas', () => {
  it('accepts plain schemas everywhere a schema is registered', () => {
    new MessageHandlerConfigBuilder<Message, undefined>().addConfig(MESSAGE_SCHEMA, noop)
    new MessageHandlerConfig<Message, undefined>(MESSAGE_SCHEMA, noop)
    expectTypeOf<readonly (typeof MESSAGE_SCHEMA)[]>().toExtend<
      QueuePublisherOptions<never, never, Message>['messageSchemas']
    >()
  })

  it('refuses an already precompiled schema on a handler config', () => {
    // @ts-expect-error precompiled schemas are rejected: the toolkit compiles what it is given
    new MessageHandlerConfig<Message, undefined>(PRECOMPILED_MESSAGE_SCHEMA, noop)
  })

  it('refuses an already precompiled schema on a handler config builder', () => {
    new MessageHandlerConfigBuilder<Message, undefined>().addConfig(
      // @ts-expect-error precompiled schemas are rejected: the toolkit compiles what it is given
      PRECOMPILED_MESSAGE_SCHEMA,
      noop,
    )
  })

  it('refuses an already precompiled schema on publisher options', () => {
    expectTypeOf<readonly (typeof PRECOMPILED_MESSAGE_SCHEMA)[]>().not.toExtend<
      QueuePublisherOptions<never, never, Message>['messageSchemas']
    >()
  })
})
