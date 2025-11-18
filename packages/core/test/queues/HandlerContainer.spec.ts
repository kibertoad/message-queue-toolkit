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

// EventBridge-style envelope schemas (for testing 3-param version)
const USER_ENVELOPE_SCHEMA = z.object({
  version: z.string(),
  id: z.string(),
  'detail-type': z.literal('user.created'),
  source: z.string(),
  time: z.string(),
  detail: USER_MESSAGE_SCHEMA,
})

const ORDER_ENVELOPE_SCHEMA = z.object({
  version: z.string(),
  id: z.string(),
  'detail-type': z.literal('order.placed'),
  source: z.string(),
  time: z.string(),
  detail: ORDER_MESSAGE_SCHEMA,
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
      expect(configs[0]?.envelopeSchema).toBeUndefined()
      expect(configs[1]?.schema).toBe(ORDER_MESSAGE_SCHEMA)
      expect(configs[1]?.envelopeSchema).toBeUndefined()
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

  describe('3-param addConfig (envelope + payload schemas)', () => {
    it('should build configs with separate envelope and payload schemas', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      const configs = builder
        .addConfig(USER_ENVELOPE_SCHEMA, USER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .addConfig(ORDER_ENVELOPE_SCHEMA, ORDER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .build()

      expect(configs).toHaveLength(2)

      // First config
      expect(configs[0]?.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(configs[0]?.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)

      // Second config
      expect(configs[1]?.schema).toBe(ORDER_MESSAGE_SCHEMA)
      expect(configs[1]?.envelopeSchema).toBe(ORDER_ENVELOPE_SCHEMA)
    })

    it('should build configs with options in 3-param version', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      const messageLogFormatter = (message: UserMessage) => ({
        userId: message.userId,
      })

      const configs = builder
        .addConfig(
          USER_ENVELOPE_SCHEMA,
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
      expect(configs[0]?.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(configs[0]?.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)
      expect(configs[0]?.messageLogFormatter).toBe(messageLogFormatter)
    })

    it('should support mixing 2-param and 3-param versions', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      const configs = builder
        .addConfig(USER_ENVELOPE_SCHEMA, USER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .addConfig(ORDER_MESSAGE_SCHEMA, (message, context) => {
          context.processedMessages.push(message)
          return Promise.resolve({ result: 'success' as const })
        })
        .build()

      expect(configs).toHaveLength(2)
      expect(configs[0]?.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)
      expect(configs[1]?.envelopeSchema).toBeUndefined()
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
        USER_ENVELOPE_SCHEMA,
      )

      expect(config.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(config.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)
      expect(config.handler).toBe(handler)
      expect(config.messageLogFormatter).toBe(messageLogFormatter)
      expect(config.preHandlers).toEqual([])
    })

    it('should use default log formatter when not provided', () => {
      const handler = () => Promise.resolve({ result: 'success' as const })

      const config = new MessageHandlerConfig(USER_MESSAGE_SCHEMA, handler)

      const testMessage: UserMessage = {
        type: 'user.created',
        userId: '123',
        email: 'test@example.com',
      }

      expect(config.messageLogFormatter(testMessage)).toEqual(testMessage)
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
        messageTypeField: 'type',
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
        messageTypeField: 'type',
      })

      expect(() => container.resolveHandler('unknown.type')).toThrow(
        'Unsupported message type: unknown.type',
      )
    })
  })

  describe('routing with 3-param configs (with envelope schema)', () => {
    it('should route messages using envelope schema', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_ENVELOPE_SCHEMA, USER_MESSAGE_SCHEMA, () =>
          Promise.resolve({
            result: 'success' as const,
          }),
        )
        .addConfig(ORDER_ENVELOPE_SCHEMA, ORDER_MESSAGE_SCHEMA, () =>
          Promise.resolve({
            result: 'success' as const,
          }),
        )
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeField: 'detail-type',
      })

      const userHandler = container.resolveHandler('user.created')
      const orderHandler = container.resolveHandler('order.placed')

      // Envelope schema should be used for routing
      expect(userHandler.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)
      expect(orderHandler.envelopeSchema).toBe(ORDER_ENVELOPE_SCHEMA)

      // Payload schema should be stored for validation
      expect(userHandler.schema).toBe(USER_MESSAGE_SCHEMA)
      expect(orderHandler.schema).toBe(ORDER_MESSAGE_SCHEMA)
    })

    it('should route using envelope schema even when different from payload', () => {
      // Create an envelope schema with different type field name
      const customEnvelope = z.object({
        'detail-type': z.literal('custom.user.event'),
        payload: USER_MESSAGE_SCHEMA,
      })

      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(customEnvelope, USER_MESSAGE_SCHEMA, () =>
          Promise.resolve({
            result: 'success' as const,
          }),
        )
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeField: 'detail-type',
      })

      // Should route using envelope schema's 'detail-type' value
      const handler = container.resolveHandler('custom.user.event')

      expect(handler.envelopeSchema).toBe(customEnvelope)
      expect(handler.schema).toBe(USER_MESSAGE_SCHEMA)
    })
  })

  describe('routing with mixed configs', () => {
    it('should handle mix of envelope and non-envelope configs', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_ENVELOPE_SCHEMA, USER_MESSAGE_SCHEMA, () =>
          Promise.resolve({
            result: 'success' as const,
          }),
        )
        .addConfig(ORDER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      // First container uses 'detail-type' for envelope-based routing
      const envelopeContainer = new HandlerContainer({
        messageHandlers: [configs[0]!],
        messageTypeField: 'detail-type',
      })

      // Second container uses 'type' for payload-based routing
      const payloadContainer = new HandlerContainer({
        messageHandlers: [configs[1]!],
        messageTypeField: 'type',
      })

      const userHandler = envelopeContainer.resolveHandler('user.created')
      const orderHandler = payloadContainer.resolveHandler('order.placed')

      expect(userHandler.envelopeSchema).toBe(USER_ENVELOPE_SCHEMA)
      expect(orderHandler.envelopeSchema).toBeUndefined()
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
        messageTypeField: 'type',
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
