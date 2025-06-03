import { expectTypeOf } from 'vitest'
import z from 'zod'
import type { TopicConfig } from '../types.js'
import { KafkaHandlerConfig } from './KafkaHandlerConfig.js'
import { KafkaHandlerRoutingBuilder } from './KafkaHandlerRoutingBuilder.js'

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
    // Given
    const builder = new KafkaHandlerRoutingBuilder<TopicsConfig>()
      .addConfig(
        'all',
        new KafkaHandlerConfig(CREATE_SCHEMA, (message) => {
          expectTypeOf(message).toEqualTypeOf<z.infer<typeof CREATE_SCHEMA>>()
        }),
      )
      .addConfig(
        'all',
        new KafkaHandlerConfig(UPDATE_SCHEMA, (message) => {
          expectTypeOf(message).toEqualTypeOf<z.infer<typeof UPDATE_SCHEMA>>()
        }),
      )
      .addConfig(
        'empty',
        new KafkaHandlerConfig(EMPTY_SCHEMA, (message) => {
          expectTypeOf(message).toEqualTypeOf<z.infer<typeof EMPTY_SCHEMA>>()
        }),
      )

    // When
    const routing = builder.build()

    // Then
    expect(routing).toEqual({
      all: [
        new KafkaHandlerConfig(CREATE_SCHEMA, expect.any(Function)),
        new KafkaHandlerConfig(UPDATE_SCHEMA, expect.any(Function)),
      ],
      empty: [new KafkaHandlerConfig(EMPTY_SCHEMA, expect.any(Function))],
    })
  })
})
