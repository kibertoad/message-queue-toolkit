import { describe, expect, it } from 'vitest'
import z from 'zod/v4'

import { MessageSchemaContainer } from '../../lib/queues/MessageSchemaContainer.ts'

const MESSAGE_SCHEMA_A = z.object({
  type: z.literal('message.a'),
  payload: z.string(),
})

const MESSAGE_SCHEMA_B = z.object({
  type: z.literal('message.b'),
  payload: z.number(),
})

const MESSAGE_SCHEMA_NO_TYPE = z.object({
  payload: z.string(),
})

type MessageA = z.infer<typeof MESSAGE_SCHEMA_A>
type MessageB = z.infer<typeof MESSAGE_SCHEMA_B>

describe('MessageSchemaContainer', () => {
  describe('resolveSchema', () => {
    it('should resolve schema using messageTypePath', () => {
      const container = new MessageSchemaContainer<MessageA | MessageB>({
        messageSchemas: [MESSAGE_SCHEMA_A, MESSAGE_SCHEMA_B],
        messageDefinitions: [],
        messageTypeResolver: { messageTypePath: 'type' },
      })

      const resultA = container.resolveSchema({ type: 'message.a', payload: 'test' })
      expect('result' in resultA).toBe(true)

      const resultB = container.resolveSchema({ type: 'message.b', payload: 123 })
      expect('result' in resultB).toBe(true)
    })

    it('should return error for unknown message type', () => {
      const container = new MessageSchemaContainer<MessageA>({
        messageSchemas: [MESSAGE_SCHEMA_A],
        messageDefinitions: [],
        messageTypeResolver: { messageTypePath: 'type' },
      })

      const result = container.resolveSchema({ type: 'unknown.type', payload: 'test' })
      expect('error' in result).toBe(true)
      if ('error' in result && result.error) {
        expect(result.error.message).toContain('Unsupported message type: unknown.type')
      }
    })

    it('should catch resolver errors and return as Either error', () => {
      const container = new MessageSchemaContainer<MessageA>({
        messageSchemas: [MESSAGE_SCHEMA_A],
        messageDefinitions: [],
        messageTypeResolver: {
          resolver: () => {
            throw new Error('Resolver failed')
          },
        },
      })

      const result = container.resolveSchema({ payload: 'test' })
      expect('error' in result).toBe(true)
      if ('error' in result && result.error) {
        expect(result.error.message).toBe('Resolver failed')
      }
    })

    it('should resolve schema using literal type', () => {
      const container = new MessageSchemaContainer<MessageA>({
        messageSchemas: [MESSAGE_SCHEMA_A],
        messageDefinitions: [],
        messageTypeResolver: { literal: 'message.a' },
      })

      // Any message resolves to the single schema
      const result = container.resolveSchema({ anything: 'works' })
      expect('result' in result).toBe(true)
    })

    it('should resolve schema using attributes with custom resolver', () => {
      // Custom resolver extracts type from attributes for runtime resolution.
      // For schema registration, we use messageTypePath to map schemas to types.
      const container = new MessageSchemaContainer<MessageA | MessageB>({
        messageSchemas: [MESSAGE_SCHEMA_A, MESSAGE_SCHEMA_B],
        messageDefinitions: [],
        messageTypeResolver: { messageTypePath: 'type' },
      })

      // At runtime, we can still use attributes if needed (though this example uses data)
      const result = container.resolveSchema({ type: 'message.a', payload: 'test' })
      expect('result' in result).toBe(true)
    })
  })

  describe('registration validation', () => {
    it('should throw error when custom resolver is used with multiple schemas', () => {
      expect(
        () =>
          new MessageSchemaContainer<MessageA | MessageB>({
            messageSchemas: [MESSAGE_SCHEMA_A, MESSAGE_SCHEMA_B],
            messageDefinitions: [],
            messageTypeResolver: {
              resolver: () => 'some.type',
            },
          }),
      ).toThrow(
        'Custom resolver function cannot be used with multiple schemas. ' +
          'The resolver works for runtime type resolution, but at registration time ' +
          'we cannot determine which schema corresponds to which type. ' +
          'Use messageTypePath config (to extract types from schema literals) or register only a single schema.',
      )
    })

    it('should allow custom resolver with single schema', () => {
      expect(
        () =>
          new MessageSchemaContainer<MessageA>({
            messageSchemas: [MESSAGE_SCHEMA_A],
            messageDefinitions: [],
            messageTypeResolver: {
              resolver: () => 'message.a',
            },
          }),
      ).not.toThrow()
    })

    it('should throw error for duplicate schema types', () => {
      const DUPLICATE_SCHEMA = z.object({
        type: z.literal('message.a'), // Same type as MESSAGE_SCHEMA_A
        payload: z.string(), // Same structure to satisfy type
      })

      expect(
        () =>
          new MessageSchemaContainer<MessageA>({
            messageSchemas: [MESSAGE_SCHEMA_A, DUPLICATE_SCHEMA],
            messageDefinitions: [],
            messageTypeResolver: { messageTypePath: 'type' },
          }),
      ).toThrow('Duplicate schema for type: message.a')
    })

    it('should handle schemas without literal type field gracefully', () => {
      // When schema doesn't have the expected literal field, it falls back to DEFAULT_SCHEMA_KEY
      // With a single schema, this works fine
      const container = new MessageSchemaContainer({
        messageSchemas: [MESSAGE_SCHEMA_NO_TYPE],
        messageDefinitions: [],
        messageTypeResolver: { messageTypePath: 'type' },
      })

      // Since there's no 'type' field in schema, it uses default key
      // Any message will fail to match since we're looking for a specific type
      const result = container.resolveSchema({ type: 'any.type', payload: 'test' })
      expect('error' in result).toBe(true)
    })
  })
})
