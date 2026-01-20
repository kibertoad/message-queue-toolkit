import { describe, expect, it } from 'vitest'
import z from 'zod/v4'

import {
  HandlerContainer,
  MessageHandlerConfig,
  MessageHandlerConfigBuilder,
} from '../../lib/queues/HandlerContainer.ts'

// Test message types
type UserMessage = {
  type: 'user.created'
  userId: string
  email: string
}

type OrderMessage = {
  type: 'order.placed'
  orderId: string
  amount: number
}

type SupportedMessages = UserMessage | OrderMessage

// Test context
type TestContext = {
  processedMessages: unknown[]
}

// Test schemas
const USER_MESSAGE_SCHEMA = z.object({
  type: z.literal('user.created'),
  userId: z.string(),
  email: z.string(),
})

const ORDER_MESSAGE_SCHEMA = z.object({
  type: z.literal('order.placed'),
  orderId: z.string(),
  amount: z.number(),
})

describe('MessageHandlerConfigBuilder', () => {
  describe('2-param addConfig (backward compatible)', () => {
    it('should build configs with schema used for both routing and validation', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      const configs = builder
        .addConfig(USER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .addConfig(ORDER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .build()

      expect(configs).toHaveLength(2)
      expect(configs[0]?.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(configs[1]?.schema).toBe(ORDER_MESSAGE_SCHEMA)
    })

    it('should build configs with options', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      const messageLogFormatter = (message: UserMessage) => ({
        userId: message.userId,
      })

      const configs = builder
        .addConfig(
          USER_MESSAGE_SCHEMA,
          (message, context) => {
            context.processedMessages.push(message)
            return Promise.resolve({ result: 'success' as const })
          },
          {
            messageLogFormatter,
          },
        )
        .build()

      expect(configs).toHaveLength(1)
      expect(configs[0]?.messageLogFormatter).toBe(messageLogFormatter)
    })
  })

  describe('MessageHandlerConfig', () => {
    it('should create config with all properties', () => {
      const handler = () => Promise.resolve({ result: 'success' as const })
      const messageLogFormatter = (message: UserMessage) => ({ userId: message.userId })

      const config = new MessageHandlerConfig(
        USER_MESSAGE_SCHEMA,
        handler,
        {
          messageLogFormatter,
          preHandlers: [],
        },
        undefined,
      )

      expect(config.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(config.handler).toBe(handler)
      expect(config.messageLogFormatter).toBe(messageLogFormatter)
      expect(config.preHandlers).toEqual([])
    })
  })
})

describe('HandlerContainer', () => {
  describe('routing with 2-param configs (no envelope schema)', () => {
    it('should route messages using payload schema', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .addConfig(ORDER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeResolver: { messageTypePath: 'type' },
      })

      const userHandler = container.resolveHandler('user.created')
      const orderHandler = container.resolveHandler('order.placed')

      expect(userHandler.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(orderHandler.schema).toBe(ORDER_MESSAGE_SCHEMA)
    })

    it('should throw error for unsupported message type', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeResolver: { messageTypePath: 'type' },
      })

      expect(() => container.resolveHandler('unknown.type')).toThrow(
        'Unsupported message type: unknown.type',
      )
    })
  })

  describe('resolveMessageType', () => {
    describe('with messageTypePath resolver', () => {
      it('should extract message type from the specified field', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: { messageTypePath: 'type' },
        })

        const messageType = container.resolveMessageType({
          type: 'user.created',
          userId: '1',
          email: 'test@test.com',
        })
        expect(messageType).toBe('user.created')
      })

      it('should throw error when field is missing', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: { messageTypePath: 'type' },
        })

        expect(() => container.resolveMessageType({ userId: '1' })).toThrow(
          "Unable to resolve message type: path 'type' not found in message data",
        )
      })
    })

    describe('with messageTypeResolver', () => {
      it('should use literal resolver', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: { literal: 'user.created' },
        })

        const messageType = container.resolveMessageType({})
        expect(messageType).toBe('user.created')
      })

      it('should use messageTypePath resolver', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: { messageTypePath: 'type' },
        })

        const messageType = container.resolveMessageType({ type: 'user.created' })
        expect(messageType).toBe('user.created')
      })

      it('should use custom resolver function', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(
            USER_MESSAGE_SCHEMA,
            () => Promise.resolve({ result: 'success' as const }),
            { messageType: 'user.created' }, // Required when using custom resolver
          )
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: {
            resolver: ({ messageAttributes }) => {
              const eventType = messageAttributes?.eventType as string
              if (!eventType) throw new Error('eventType required')
              return eventType === 'USER_CREATED' ? 'user.created' : eventType
            },
          },
        })

        const messageType = container.resolveMessageType({}, { eventType: 'USER_CREATED' })
        expect(messageType).toBe('user.created')
      })

      it('should pass messageAttributes to custom resolver', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(
            USER_MESSAGE_SCHEMA,
            () => Promise.resolve({ result: 'success' as const }),
            { messageType: 'user.created' }, // Required when using custom resolver
          )
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: {
            resolver: ({ messageData, messageAttributes }) => {
              // Prefer attributes over data
              if (messageAttributes?.type) {
                return messageAttributes.type as string
              }
              const data = messageData as { type?: string }
              if (!data.type) throw new Error('type required')
              return data.type
            },
          },
        })

        // From attributes
        expect(container.resolveMessageType({}, { type: 'user.created' })).toBe('user.created')
        // From data when no attributes
        expect(container.resolveMessageType({ type: 'user.created' })).toBe('user.created')
      })
    })

    describe('with no configuration', () => {
      it('should throw error when messageTypeResolver is not configured', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(
            USER_MESSAGE_SCHEMA,
            () => Promise.resolve({ result: 'success' as const }),
            { messageType: 'user.created' }, // Explicit type to allow handler registration
          )
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
        })

        // Handler is registered but runtime resolution fails
        expect(() => container.resolveMessageType({ type: 'user.created' })).toThrow(
          'Unable to resolve message type: messageTypeResolver is not configured',
        )
      })

      it('should throw error during registration if message type cannot be determined', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
          .build()

        // Without messageTypePath, literal resolver, or explicit messageType, registration should fail
        expect(
          () =>
            new HandlerContainer({
              messageHandlers: configs,
            }),
        ).toThrow(
          'Unable to determine message type for handler at registration time. ' +
            'Either provide explicit messageType in handler options (required for custom resolver functions), ' +
            'use a literal resolver, or ensure the schema has a literal type field matching messageTypePath.',
        )
      })

      it('should throw error for duplicate message types', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }), {
            messageType: 'duplicate.type',
          })
          .addConfig(ORDER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }), {
            messageType: 'duplicate.type',
          })
          .build()

        expect(
          () =>
            new HandlerContainer({
              messageHandlers: configs,
            }),
        ).toThrow('Duplicate handler for message type: duplicate.type')
      })
    })

    describe('with explicit messageType in handler options', () => {
      it('should use explicit messageType over schema extraction', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(
            USER_MESSAGE_SCHEMA,
            () => Promise.resolve({ result: 'success' as const }),
            { messageType: 'custom.type' }, // Override schema-derived type
          )
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: { messageTypePath: 'type' },
        })

        // Handler is registered under 'custom.type', not 'user.created'
        expect(() => container.resolveHandler('user.created')).toThrow(
          'Unsupported message type: user.created',
        )
        const handler = container.resolveHandler('custom.type')
        expect(handler.schema).toBe(USER_MESSAGE_SCHEMA)
      })

      it('should support multiple handlers with explicit types', () => {
        const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
          .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }), {
            messageType: 'storage.object.created',
          })
          .addConfig(ORDER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }), {
            messageType: 'storage.object.deleted',
          })
          .build()

        const container = new HandlerContainer({
          messageHandlers: configs,
          messageTypeResolver: {
            resolver: ({ messageAttributes }) => {
              const eventType = messageAttributes?.eventType as string
              if (eventType === 'OBJECT_FINALIZE') return 'storage.object.created'
              if (eventType === 'OBJECT_DELETE') return 'storage.object.deleted'
              throw new Error(`Unknown event type: ${eventType}`)
            },
          },
        })

        // Verify handlers are correctly registered
        expect(container.resolveHandler('storage.object.created').schema).toBe(USER_MESSAGE_SCHEMA)
        expect(container.resolveHandler('storage.object.deleted').schema).toBe(ORDER_MESSAGE_SCHEMA)

        // Verify runtime resolution works with the resolver
        expect(container.resolveMessageType({}, { eventType: 'OBJECT_FINALIZE' })).toBe(
          'storage.object.created',
        )
        expect(container.resolveMessageType({}, { eventType: 'OBJECT_DELETE' })).toBe(
          'storage.object.deleted',
        )
      })
    })
  })

  describe('handler execution', () => {
    it('should execute handler with correct context', async () => {
      const context: TestContext = {
        processedMessages: [],
      }

      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, (message, ctx) => {
          ctx.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeResolver: { messageTypePath: 'type' },
      })

      const handler = container.resolveHandler('user.created')

      const testMessage: UserMessage = {
        type: 'user.created',
        userId: '123',
        email: 'test@example.com',
      }

      const result = await handler.handler(
        testMessage,
        context,
        { preHandlerOutput: undefined, barrierOutput: undefined },
        undefined,
      )

      expect(result).toEqual({ result: 'success' })
      expect(context.processedMessages).toHaveLength(1)
      expect(context.processedMessages[0]).toEqual(testMessage)
    })
  })
})
