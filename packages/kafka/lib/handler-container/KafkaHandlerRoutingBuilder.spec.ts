import type { Message } from '@platformatic/kafka'
import { expectTypeOf } from 'vitest'
import z from 'zod/v4'
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

type ExecutionContext = {
  hello: string
}

describe('KafkaHandlerRoutingBuilder', () => {
  it('should build routing config', () => {
    type ExpectedMessage<MessageValue> = Message<string, MessageValue, string, string>

    // Given
    const builder = new KafkaHandlerRoutingBuilder<TopicsConfig, ExecutionContext>()
      .addConfig(
        'all',
        new KafkaHandlerConfig(CREATE_SCHEMA, (message, executionContext, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.output<typeof CREATE_SCHEMA>>>()
          expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        }),
      )
      .addConfig(
        'all',
        new KafkaHandlerConfig(UPDATE_SCHEMA, (message, executionContext, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.output<typeof UPDATE_SCHEMA>>>()
          expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        }),
      )
      .addConfig(
        'empty',
        new KafkaHandlerConfig(EMPTY_SCHEMA, (message, executionContext, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<ExpectedMessage<z.output<typeof EMPTY_SCHEMA>>>()
          expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
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
