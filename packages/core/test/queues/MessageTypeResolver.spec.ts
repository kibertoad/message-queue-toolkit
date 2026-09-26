import { describe, expect, it, vi } from 'vitest'
import { z } from 'zod/v4'

import {
  extractMessageTypeFromSchema,
  isMessageTypeLiteralConfig,
  isMessageTypePathConfig,
  isMessageTypeResolverFnConfig,
  resolveMessageType,
} from '../../lib/queues/MessageTypeResolver.ts'

describe('MessageTypeResolver', () => {
  describe('type guards', () => {
    it('isMessageTypePathConfig should return true for messageTypePath config', () => {
      expect(isMessageTypePathConfig({ messageTypePath: 'type' })).toBe(true)
      expect(isMessageTypePathConfig({ literal: 'test' })).toBe(false)
      expect(isMessageTypePathConfig({ resolver: () => 'test' })).toBe(false)
    })

    it('isMessageTypeLiteralConfig should return true for literal config', () => {
      expect(isMessageTypeLiteralConfig({ literal: 'test' })).toBe(true)
      expect(isMessageTypeLiteralConfig({ messageTypePath: 'type' })).toBe(false)
      expect(isMessageTypeLiteralConfig({ resolver: () => 'test' })).toBe(false)
    })

    it('isMessageTypeResolverFnConfig should return true for resolver config', () => {
      expect(isMessageTypeResolverFnConfig({ resolver: () => 'test' })).toBe(true)
      expect(isMessageTypeResolverFnConfig({ messageTypePath: 'type' })).toBe(false)
      expect(isMessageTypeResolverFnConfig({ literal: 'test' })).toBe(false)
    })
  })

  describe('resolveMessageType', () => {
    describe('literal mode', () => {
      it('should return the literal value regardless of message content', () => {
        const config = { literal: 'order.created' }

        expect(resolveMessageType(config, { messageData: {} })).toBe('order.created')
        expect(resolveMessageType(config, { messageData: { type: 'different' } })).toBe(
          'order.created',
        )
        expect(resolveMessageType(config, { messageData: null })).toBe('order.created')
      })
    })

    describe('messageTypePath mode', () => {
      it('should extract type from the specified field path', () => {
        const config = { messageTypePath: 'type' }

        expect(resolveMessageType(config, { messageData: { type: 'user.created' } })).toBe(
          'user.created',
        )
      })

      it('should work with different field names', () => {
        const config = { messageTypePath: 'eventType' }

        expect(resolveMessageType(config, { messageData: { eventType: 'order.placed' } })).toBe(
          'order.placed',
        )
      })

      it('should work with kebab-case field names', () => {
        const config = { messageTypePath: 'detail-type' }

        expect(
          resolveMessageType(config, { messageData: { 'detail-type': 'user.presence' } }),
        ).toBe('user.presence')
      })

      it('should support nested paths with dot notation', () => {
        const config = { messageTypePath: 'metadata.type' }

        expect(
          resolveMessageType(config, { messageData: { metadata: { type: 'nested.event' } } }),
        ).toBe('nested.event')
      })

      it('should support deeply nested paths', () => {
        const config = { messageTypePath: 'envelope.header.eventType' }

        expect(
          resolveMessageType(config, {
            messageData: { envelope: { header: { eventType: 'deep.nested.event' } } },
          }),
        ).toBe('deep.nested.event')
      })

      it('should throw error when path is missing', () => {
        const config = { messageTypePath: 'type' }

        expect(() => resolveMessageType(config, { messageData: {} })).toThrow(
          "Unable to resolve message type: path 'type' not found in message data",
        )
      })

      it('should throw error when nested path is missing', () => {
        const config = { messageTypePath: 'metadata.type' }

        expect(() => resolveMessageType(config, { messageData: { metadata: {} } })).toThrow(
          "Unable to resolve message type: path 'metadata.type' not found in message data",
        )
      })

      it('should throw error when messageData is undefined', () => {
        const config = { messageTypePath: 'type' }

        expect(() => resolveMessageType(config, { messageData: undefined })).toThrow(
          "Unable to resolve message type: path 'type' not found in message data",
        )
      })

      it('should throw error when messageData is null', () => {
        const config = { messageTypePath: 'type' }

        expect(() => resolveMessageType(config, { messageData: null })).toThrow(
          "Unable to resolve message type: path 'type' not found in message data",
        )
      })

      it('should throw error when value is a number', () => {
        const config = { messageTypePath: 'type' }

        expect(() => resolveMessageType(config, { messageData: { type: 123 } })).toThrow(
          "Unable to resolve message type: path 'type' contains a non-string value (got number)",
        )
      })

      it('should throw error when value is a boolean', () => {
        const config = { messageTypePath: 'type' }

        expect(() => resolveMessageType(config, { messageData: { type: true } })).toThrow(
          "Unable to resolve message type: path 'type' contains a non-string value (got boolean)",
        )
      })

      it('should throw error when value is an object', () => {
        const config = { messageTypePath: 'type' }

        expect(() =>
          resolveMessageType(config, { messageData: { type: { nested: 'value' } } }),
        ).toThrow(
          "Unable to resolve message type: path 'type' contains a non-string value (got object)",
        )
      })

      it('should throw error when nested path value is not a string', () => {
        const config = { messageTypePath: 'metadata.type' }

        expect(() =>
          resolveMessageType(config, { messageData: { metadata: { type: 42 } } }),
        ).toThrow(
          "Unable to resolve message type: path 'metadata.type' contains a non-string value (got number)",
        )
      })
    })

    describe('custom resolver mode', () => {
      it('should use the custom resolver function', () => {
        const config = {
          resolver: ({ messageData }: { messageData: unknown }) => {
            const data = messageData as { metadata?: { eventName?: string } }
            return data.metadata?.eventName ?? 'default'
          },
        }

        expect(
          resolveMessageType(config, { messageData: { metadata: { eventName: 'custom.event' } } }),
        ).toBe('custom.event')
      })

      it('should pass messageAttributes to the resolver', () => {
        const config = {
          resolver: ({ messageAttributes }: { messageAttributes?: Record<string, unknown> }) => {
            const eventType = messageAttributes?.eventType as string
            if (!eventType) throw new Error('eventType required')
            return eventType
          },
        }

        expect(
          resolveMessageType(config, {
            messageData: {},
            messageAttributes: { eventType: 'OBJECT_FINALIZE' },
          }),
        ).toBe('OBJECT_FINALIZE')
      })

      it('should support mapping event types', () => {
        const config = {
          resolver: ({ messageAttributes }: { messageAttributes?: Record<string, unknown> }) => {
            const eventType = messageAttributes?.eventType as string
            if (eventType === 'OBJECT_FINALIZE') return 'storage.object.created'
            if (eventType === 'OBJECT_DELETE') return 'storage.object.deleted'
            return eventType ?? 'unknown'
          },
        }

        expect(
          resolveMessageType(config, {
            messageData: {},
            messageAttributes: { eventType: 'OBJECT_FINALIZE' },
          }),
        ).toBe('storage.object.created')

        expect(
          resolveMessageType(config, {
            messageData: {},
            messageAttributes: { eventType: 'OBJECT_DELETE' },
          }),
        ).toBe('storage.object.deleted')
      })

      it('should allow resolver to throw custom errors', () => {
        const config = {
          resolver: () => {
            throw new Error('Custom error: event type missing')
          },
        }

        expect(() => resolveMessageType(config, { messageData: {} })).toThrow(
          'Custom error: event type missing',
        )
      })
    })
  })

  describe('extractMessageTypeFromSchema', () => {
    const fromJsonSchema = (jsonSchema: Record<string, unknown>) => ({
      '~standard': { vendor: 'test', jsonSchema: { input: () => jsonSchema } },
    })

    it('should extract literal value from a zod object schema', () => {
      const schema = z.object({ type: z.literal('user.created'), id: z.string() })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('user.created')
    })

    it('should return undefined when field path is undefined', () => {
      const schema = z.object({ type: z.literal('user.created') })

      expect(extractMessageTypeFromSchema(schema, undefined)).toBeUndefined()
    })

    it('should return undefined when field is not in schema', () => {
      const schema = z.object({ type: z.literal('user.created') })

      expect(extractMessageTypeFromSchema(schema, 'eventType')).toBeUndefined()
    })

    it('should return undefined when schema does not implement Standard JSON Schema', () => {
      expect(extractMessageTypeFromSchema({}, 'type')).toBeUndefined()
      expect(extractMessageTypeFromSchema(undefined, 'type')).toBeUndefined()
    })

    it('should return undefined when field is not a literal', () => {
      const schema = z.object({ type: z.string() })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should return undefined when field is a multi-value literal', () => {
      const schema = z.object({ type: z.literal(['a', 'b']) })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should extract literal value from nested path', () => {
      const schema = z.object({ metadata: z.object({ type: z.literal('nested.event') }) })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBe('nested.event')
    })

    it('should extract literal value from deeply nested path', () => {
      const schema = z.object({
        envelope: z.object({ header: z.object({ eventType: z.literal('deep.nested.event') }) }),
      })

      expect(extractMessageTypeFromSchema(schema, 'envelope.header.eventType')).toBe(
        'deep.nested.event',
      )
    })

    it('should follow $refs for nested schemas registered with an id', () => {
      const Metadata = z.object({ type: z.literal('ref.event') }).meta({ id: 'Metadata' })
      const schema = z.object({ metadata: Metadata, previous: Metadata })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBe('ref.event')
    })

    it('should extract literal value from a transformed object schema', () => {
      const schema = z.object({ type: z.literal('piped.event') }).transform((value) => value)

      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('piped.event')
    })

    it('should extract literal value when other fields cannot be represented in JSON Schema', () => {
      const schema = z.object({
        type: z.literal('dated.event'),
        at: z.date(),
        custom: z.custom<string>(() => true),
      })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('dated.event')
    })

    it('should return undefined when nested path does not exist', () => {
      const schema = z.object({ metadata: z.object({}) })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBeUndefined()
    })

    it('should return undefined when intermediate path is not an object schema', () => {
      const schema = z.object({ metadata: z.literal('string') })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBeUndefined()
    })

    it('should return undefined when value is a number', () => {
      const schema = z.object({ type: z.literal(123) })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should return undefined when value is a boolean', () => {
      const schema = z.object({ type: z.literal(true) })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should return undefined when const value is an object', () => {
      const schema = fromJsonSchema({
        type: 'object',
        properties: { type: { const: { nested: 'value' } } },
      })

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should return undefined when nested path value is not a string', () => {
      const schema = z.object({ metadata: z.object({ type: z.literal(42) }) })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBeUndefined()
    })

    it('should follow draft-07 definitions refs and ignore unresolvable refs', () => {
      const schema = fromJsonSchema({
        type: 'object',
        properties: {
          metadata: { $ref: '#/definitions/Metadata' },
          external: { $ref: 'https://example.com/schema.json' },
          missing: { $ref: '#/definitions/Missing' },
        },
        definitions: {
          Metadata: { type: 'object', properties: { type: { const: 'defs.event' } } },
        },
      })

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBe('defs.event')
      expect(extractMessageTypeFromSchema(schema, 'external.type')).toBeUndefined()
      expect(extractMessageTypeFromSchema(schema, 'missing.type')).toBeUndefined()
    })

    it('should convert each schema to JSON Schema only once', () => {
      const input = vi.fn(() => ({ type: 'object', properties: { type: { const: 'cached' } } }))
      const schema = { '~standard': { vendor: 'test', jsonSchema: { input } } }

      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('cached')
      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('cached')
      expect(input).toHaveBeenCalledTimes(1)
    })

    it('should pass lenient library options for known vendors', () => {
      const input = vi.fn(() => ({ type: 'object', properties: { type: { const: 'a' } } }))

      for (const vendor of ['zod', 'arktype', 'valibot', 'other']) {
        extractMessageTypeFromSchema({ '~standard': { vendor, jsonSchema: { input } } }, 'type')
      }

      const libraryOptions = input.mock.calls.map(
        (call) =>
          (call as unknown as [{ libraryOptions?: Record<string, unknown> }])[0].libraryOptions,
      )
      expect(libraryOptions[0]).toEqual({ unrepresentable: 'any' })
      expect(
        (libraryOptions[1]!.fallback as (ctx: { base: unknown }) => unknown)({ base: 1 }),
      ).toBe(1)
      expect(libraryOptions[2]).toEqual({ errorMode: 'ignore' })
      expect(libraryOptions[3]).toBeUndefined()
    })
  })
})
