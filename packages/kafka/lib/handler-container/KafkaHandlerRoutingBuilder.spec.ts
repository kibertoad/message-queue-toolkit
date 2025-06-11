import type { Message } from '@platformatic/kafka'
import { expectTypeOf } from 'vitest'
import z from 'zod'
import type { TopicConfig } from '../types.ts'
import { KafkaHandlerConfig, type RequestContext } from './KafkaHandlerConfig.ts'
import { KafkaHandlerRoutingBuilder } from './KafkaHandlerRoutingBuilder.ts'

const CREATE_SCHEMA = z.object({ type: z.literal('create') })
const UPDATE_SCHEMA = z.object({ type: z.literal('update') })
const EMPTY_SCHEMA = z.object({})

const topicsConfig = [
  { topic: 'all', schemas: [CREATE_SCHEMA, UPDATE_SCHEMA] },
  { topic: 'empty', schemas: [EMPTY_SCHEMA] },
] as const satisfies TopicConfig[]
type TopicsConfig = typeof topicsConfig

describe('KafkaHandlerRoutingBuilder', () => {
  it('should build routing config', () => {
    type ExpectedMessage<MessageValue> = Message<string, MessageValue, string, string>

    // Given
    const builder = new KafkaHandlerRoutingBuilder<TopicsConfig>()
      .addConfig(
        'all',
        new KafkaHandlerConfig(CREATE_SCHEMA, (message, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.infer<typeof CREATE_SCHEMA>>>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        }),
      )
      .addConfig(
        'all',
        new KafkaHandlerConfig(UPDATE_SCHEMA, (message, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.infer<typeof UPDATE_SCHEMA>>>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        }),
      )
      .addConfig(
        'empty',
        new KafkaHandlerConfig(EMPTY_SCHEMA, (message, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.infer<typeof EMPTY_SCHEMA>>>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        }),
      )

    // When
    const routing = builder.build()

    // Then
    expect(routing).toEqual({
      all: [
        new KafkaHandlerConfig(CREATE_SCHEMA, expect.any(Function) as any),
        new KafkaHandlerConfig(UPDATE_SCHEMA, expect.any(Function) as any),
      ],
      empty: [new KafkaHandlerConfig(EMPTY_SCHEMA, expect.any(Function) as any)],
    })
  })
})
