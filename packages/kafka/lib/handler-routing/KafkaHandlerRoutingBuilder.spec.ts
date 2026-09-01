import { expectTypeOf } from 'vitest'
import z from 'zod/v4'
import type { DeserializedMessage, RequestContext, TopicConfig } from '../types.ts'
import { KafkaBatchHandlerConfig, KafkaHandlerConfig } from './KafkaHandlerConfig.ts'
import { KafkaHandlerRoutingBuilder } from './KafkaHandlerRoutingBuilder.ts'

const CREATE_SCHEMA = z.object({ type: z.literal('create') })
const EMPTY_SCHEMA = z.object({})

const topicsConfig = [
  { topic: 'all', schema: CREATE_SCHEMA },
  { topic: 'empty', schema: EMPTY_SCHEMA },
] as const satisfies TopicConfig[]
type TopicsConfig = typeof topicsConfig

type ExecutionContext = {
  hello: string
}

describe('KafkaHandlerRoutingBuilder', () => {
  describe('batch processing disabled', () => {
    it('should build routing config', () => {
      // Given
      const createConfig = new KafkaHandlerConfig<z.output<typeof CREATE_SCHEMA>, ExecutionContext>(
        CREATE_SCHEMA,
        (message, executionContext, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<DeserializedMessage<z.output<typeof CREATE_SCHEMA>>>()
          expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        },
      )
      const emptyConfig = new KafkaHandlerConfig<z.output<typeof EMPTY_SCHEMA>, ExecutionContext>(
        EMPTY_SCHEMA,
        (message, executionContext, requestContext) => {
          expectTypeOf(message).toEqualTypeOf<DeserializedMessage<z.output<typeof EMPTY_SCHEMA>>>()
          expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
          expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
        },
      )
      const builder = new KafkaHandlerRoutingBuilder<TopicsConfig, ExecutionContext, false>()
        .addConfig('all', createConfig)
        .addConfig('empty', emptyConfig)

      // When
      const routing = builder.build()

      // Then
      expect(routing).toEqual({ all: createConfig, empty: emptyConfig })
    })
  })

  describe('batch processing enabled', () => {
    it('should build routing config', () => {
      // Given
      const createConfig = new KafkaBatchHandlerConfig<
        z.output<typeof CREATE_SCHEMA>,
        ExecutionContext
      >(CREATE_SCHEMA, (message, executionContext, requestContext) => {
        expectTypeOf(message).toEqualTypeOf<DeserializedMessage<z.output<typeof CREATE_SCHEMA>>[]>()
        expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
        expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
      })
      const emptyConfig = new KafkaBatchHandlerConfig<
        z.output<typeof EMPTY_SCHEMA>,
        ExecutionContext
      >(EMPTY_SCHEMA, (message, executionContext, requestContext) => {
        expectTypeOf(message).toEqualTypeOf<DeserializedMessage<z.output<typeof EMPTY_SCHEMA>>[]>()
        expectTypeOf(executionContext).toEqualTypeOf<ExecutionContext>()
        expectTypeOf(requestContext).toEqualTypeOf<RequestContext>()
      })
      const builder = new KafkaHandlerRoutingBuilder<TopicsConfig, ExecutionContext, true>()
        .addConfig('all', createConfig)
        .addConfig('empty', emptyConfig)

      // When
      const routing = builder.build()

      // Then
      expect(routing).toEqual({ all: createConfig, empty: emptyConfig })
    })
  })
})
