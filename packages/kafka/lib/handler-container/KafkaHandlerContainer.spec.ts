import z from 'zod/v4'
import type { TopicConfig } from '../types.ts'
import { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'
import { KafkaHandlerContainer } from './KafkaHandlerContainer.ts'
import { KafkaHandlerRoutingBuilder } from './KafkaHandlerRoutingBuilder.ts'

const CREATE_SCHEMA = z.object({
  type: z.literal('create'),
  prop: z.string().transform((val) => Number(val)),
})
const UPDATE_SCHEMA = z.object({ type: z.literal('update') })
const EMPTY_SCHEMA = z.object({})

const topicsConfig = [
  { topic: 'all', schemas: [CREATE_SCHEMA, UPDATE_SCHEMA, EMPTY_SCHEMA] },
  { topic: 'create', schemas: [CREATE_SCHEMA] },
  { topic: 'empty', schemas: [EMPTY_SCHEMA] },
] as const satisfies TopicConfig[]
type TopicsConfig = typeof topicsConfig

describe('KafkaHandlerContainer', () => {
  describe('resolveHandler', () => {
    it('should return undefined for non-existing topic', () => {
      const container = new KafkaHandlerContainer({})

      const handler = container.resolveHandler('non-existing-topic', {} as never)
      expect(handler).toBeUndefined()
    })

    it('should throw error for duplicate message types', () => {
      // Given
      const topicHandlers1 = {
        create: [
          new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()),
          new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()),
        ],
      }
      const topicHandlers2 = {
        empty: [
          new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()),
          new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()),
        ],
      }

      // When & Then
      expect(
        () => new KafkaHandlerContainer(topicHandlers1 as any, 'type'),
      ).toThrowErrorMatchingInlineSnapshot('[Error: Duplicate handler for topic create]')
      expect(
        () => new KafkaHandlerContainer(topicHandlers2 as any),
      ).toThrowErrorMatchingInlineSnapshot('[Error: Duplicate handler for topic empty]')
    })

    it('should resolve handler with message type', () => {
      // Given
      const routing = new KafkaHandlerRoutingBuilder<TopicsConfig, any>()
        .addConfig('all', new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()))
        .addConfig('all', new KafkaHandlerConfig(UPDATE_SCHEMA, () => Promise.resolve()))
        .addConfig('all', new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()))
        .addConfig('create', new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()))
        .addConfig('empty', new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()))

      // When
      const container = new KafkaHandlerContainer(routing.build(), 'type')

      // Then
      expect(container.resolveHandler('all', { type: 'create', prop: 1 })?.schema).toBe(
        CREATE_SCHEMA,
      )
      expect(container.resolveHandler('all', { type: 'update' })?.schema).toBe(UPDATE_SCHEMA)
      expect(container.resolveHandler('all', { type: 'non-existing' as any })?.schema).toBe(
        EMPTY_SCHEMA,
      )
      expect(container.resolveHandler('all', {})?.schema).toBe(EMPTY_SCHEMA)

      expect(container.resolveHandler('create', { type: 'create', prop: 1 })?.schema).toBe(
        CREATE_SCHEMA,
      )
      expect(container.resolveHandler('create', { type: 'update' as any, prop: 1 })?.schema).toBe(
        undefined,
      )
      expect(container.resolveHandler('create', {} as any)?.schema).toBe(undefined)

      expect(container.resolveHandler('empty', {} as any)?.schema).toBe(EMPTY_SCHEMA)
      expect(container.resolveHandler('empty', { type: 'create' } as any)?.schema).toBe(
        EMPTY_SCHEMA,
      )
    })

    it('should resolve handler without message type', () => {
      // Given
      const routing = new KafkaHandlerRoutingBuilder<TopicsConfig, any>()
        .addConfig('create', new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()))
        .addConfig('empty', new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()))

      // When
      const container = new KafkaHandlerContainer(routing.build())

      // Then
      expect(container.resolveHandler('create', { type: 'create', prop: 1 })?.schema).toBe(
        CREATE_SCHEMA,
      )
      expect(container.resolveHandler('create', { type: 'update' as any, prop: 1 })?.schema).toBe(
        CREATE_SCHEMA,
      )
      expect(container.resolveHandler('create', {} as any)?.schema).toBe(CREATE_SCHEMA)

      expect(container.resolveHandler('empty', {} as any)?.schema).toBe(EMPTY_SCHEMA)
      expect(container.resolveHandler('empty', { type: 'create' } as any)?.schema).toBe(
        EMPTY_SCHEMA,
      )
    })
  })

  describe('topics', () => {
    it('should not fail with empty topics', () => {
      // When
      const container = new KafkaHandlerContainer({})

      // Then
      expect(container.topics).toEqual([])
    })

    it('should return all topics', () => {
      // Given
      const routing = new KafkaHandlerRoutingBuilder<TopicsConfig, any>()
        .addConfig('all', new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()))
        .addConfig('all', new KafkaHandlerConfig(UPDATE_SCHEMA, () => Promise.resolve()))
        .addConfig('all', new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()))
        .addConfig('create', new KafkaHandlerConfig(CREATE_SCHEMA, () => Promise.resolve()))
        .addConfig('empty', new KafkaHandlerConfig(EMPTY_SCHEMA, () => Promise.resolve()))

      // When
      const container = new KafkaHandlerContainer({ ...routing.build(), another: [] }, 'type')

      // Then
      expect(container.topics).toEqual(['all', 'create', 'empty'])
    })
  })
})
