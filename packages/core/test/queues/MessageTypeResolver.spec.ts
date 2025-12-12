import { describe, expect, it } from 'vitest'

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
    it('should extract literal value from schema shape', () => {
      const schema = {
        shape: {
          type: { value: 'user.created' },
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'type')).toBe('user.created')
    })

    it('should return undefined when field path is undefined', () => {
      const schema = {
        shape: {
          type: { value: 'user.created' },
        },
      }

      expect(extractMessageTypeFromSchema(schema, undefined)).toBeUndefined()
    })

    it('should return undefined when field is not in schema shape', () => {
      const schema = {
        shape: {
          type: { value: 'user.created' },
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'eventType')).toBeUndefined()
    })

    it('should return undefined when schema has no shape', () => {
      const schema = {}

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should return undefined when field has no value', () => {
      const schema = {
        shape: {
          type: {},
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'type')).toBeUndefined()
    })

    it('should extract literal value from nested path in schema shape', () => {
      const schema = {
        shape: {
          metadata: {
            shape: {
              type: { value: 'nested.event' },
            },
          },
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBe('nested.event')
    })

    it('should extract literal value from deeply nested path', () => {
      const schema = {
        shape: {
          envelope: {
            shape: {
              header: {
                shape: {
                  eventType: { value: 'deep.nested.event' },
                },
              },
            },
          },
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'envelope.header.eventType')).toBe(
        'deep.nested.event',
      )
    })

    it('should return undefined when nested path does not exist', () => {
      const schema = {
        shape: {
          metadata: {
            shape: {},
          },
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBeUndefined()
    })

    it('should return undefined when intermediate path is not an object schema', () => {
      const schema = {
        shape: {
          metadata: { value: 'string' }, // not a nested object
        },
      }

      expect(extractMessageTypeFromSchema(schema, 'metadata.type')).toBeUndefined()
    })
  })
})
