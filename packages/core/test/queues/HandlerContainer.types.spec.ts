/**
 * TypeScript type tests for HandlerContainer and related types.
 * These tests verify that the type system correctly enforces constraints.
 *
 * Run with: npx tsc --noEmit to check types
 */

import type { Either } from '@lokalise/node-core'
import { describe, expectTypeOf, it } from 'vitest'
import z from 'zod/v4'

import {
  type HandlerConfigOptions,
  HandlerContainer,
  type HandlerContainerOptions,
  MessageHandlerConfig,
  MessageHandlerConfigBuilder,
} from '../../lib/queues/HandlerContainer.ts'
import type { MessageTypeResolverConfig } from '../../lib/queues/MessageTypeResolver.ts'

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

describe('HandlerContainer Types', () => {
  describe('MessageHandlerConfigBuilder', () => {
    it('should infer handler message type from schema', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      // Handler receives the correct message type inferred from schema
      builder.addConfig(USER_MESSAGE_SCHEMA, (message, _context) => {
        expectTypeOf(message).toEqualTypeOf<UserMessage>()
        expectTypeOf(message.userId).toBeString()
        expectTypeOf(message.email).toBeString()
        return Promise.resolve({ result: 'success' as const })
      })

      builder.addConfig(ORDER_MESSAGE_SCHEMA, (message, _context) => {
        expectTypeOf(message).toEqualTypeOf<OrderMessage>()
        expectTypeOf(message.orderId).toBeString()
        expectTypeOf(message.amount).toBeNumber()
        return Promise.resolve({ result: 'success' as const })
      })
    })

    it('should accept messageType in options', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      // messageType is optional in handler options
      builder.addConfig(
        USER_MESSAGE_SCHEMA,
        () => Promise.resolve({ result: 'success' as const }),
        { messageType: 'custom.type' },
      )

      // Other options should still work alongside messageType
      builder.addConfig(
        ORDER_MESSAGE_SCHEMA,
        () => Promise.resolve({ result: 'success' as const }),
        {
          messageType: 'another.type',
          messageLogFormatter: (message) => ({ id: message.orderId }),
        },
      )
    })

    it('should type context correctly', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      builder.addConfig(USER_MESSAGE_SCHEMA, (_message, context) => {
        expectTypeOf(context).toEqualTypeOf<TestContext>()
        expectTypeOf(context.processedMessages).toBeArray()
        return Promise.resolve({ result: 'success' as const })
      })
    })
  })

  describe('MessageHandlerConfig', () => {
    it('should store messageType as optional string', () => {
      const config = new MessageHandlerConfig(
        USER_MESSAGE_SCHEMA,
        () => Promise.resolve({ result: 'success' as const }),
        { messageType: 'explicit.type' },
      )

      expectTypeOf(config.messageType).toEqualTypeOf<string | undefined>()
    })
  })

  describe('HandlerConfigOptions', () => {
    it('should have messageType as optional', () => {
      type Options = HandlerConfigOptions<UserMessage, TestContext, undefined, unknown>

      expectTypeOf<Options['messageType']>().toEqualTypeOf<string | undefined>()
    })
  })

  describe('HandlerContainerOptions', () => {
    it('should accept messageTypeResolver config', () => {
      type Options = HandlerContainerOptions<SupportedMessages, TestContext>

      expectTypeOf<Options['messageTypeResolver']>().toEqualTypeOf<
        MessageTypeResolverConfig | undefined
      >()
    })

    it('should accept messageTypeField for legacy support', () => {
      type Options = HandlerContainerOptions<SupportedMessages, TestContext>

      expectTypeOf<Options['messageTypeField']>().toEqualTypeOf<string | undefined>()
    })
  })

  describe('HandlerContainer', () => {
    it('resolveMessageType should return string', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeField: 'type',
      })

      const result = container.resolveMessageType({ type: 'user.created' })
      expectTypeOf(result).toBeString()
    })

    it('resolveHandler should require string parameter', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeField: 'type',
      })

      // This should compile - string is required
      container.resolveHandler('user.created')

      // The parameter type should be string
      expectTypeOf(container.resolveHandler).parameter(0).toBeString()
    })

    it('resolveHandler accepts any string (runtime validation)', () => {
      const configs = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()
        .addConfig(USER_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
        .build()

      const container = new HandlerContainer({
        messageHandlers: configs,
        messageTypeField: 'type',
      })

      // Note: resolveHandler accepts any string at compile time.
      // Type validation happens at runtime - unknown types throw an error.
      // This is by design: the message type comes from resolveMessageType()
      // which extracts it from actual validated messages.
      expectTypeOf(container.resolveHandler).parameter(0).toBeString()
      expectTypeOf(container.resolveHandler).parameter(0).not.toEqualTypeOf<'user.created'>()
    })
  })

  describe('MessageTypeResolverConfig', () => {
    it('should accept messageTypePath config', () => {
      const config: MessageTypeResolverConfig = { messageTypePath: 'type' }
      expectTypeOf(config).toMatchTypeOf<MessageTypeResolverConfig>()
    })

    it('should accept literal config', () => {
      const config: MessageTypeResolverConfig = { literal: 'fixed.type' }
      expectTypeOf(config).toMatchTypeOf<MessageTypeResolverConfig>()
    })

    it('should accept resolver function config', () => {
      const config: MessageTypeResolverConfig = {
        resolver: ({ messageData, messageAttributes }) => {
          expectTypeOf(messageData).toBeUnknown()
          expectTypeOf(messageAttributes).toEqualTypeOf<Record<string, unknown> | undefined>()
          return 'resolved.type'
        },
      }
      expectTypeOf(config).toMatchTypeOf<MessageTypeResolverConfig>()
    })

    it('resolver function should return string', () => {
      const config: MessageTypeResolverConfig = {
        resolver: () => {
          // Return type must be string
          return 'type'
        },
      }

      if ('resolver' in config) {
        const result = config.resolver({ messageData: {} })
        expectTypeOf(result).toBeString()
      }
    })
  })

  describe('Handler return type', () => {
    it('should return Either with retryLater or success', () => {
      const builder = new MessageHandlerConfigBuilder<SupportedMessages, TestContext>()

      builder.addConfig(USER_MESSAGE_SCHEMA, () => {
        // Both return types should be valid
        const successResult: Either<'retryLater', 'success'> = { result: 'success' }
        const retryResult: Either<'retryLater', 'success'> = { error: 'retryLater' }

        expectTypeOf(successResult).toMatchTypeOf<Either<'retryLater', 'success'>>()
        expectTypeOf(retryResult).toMatchTypeOf<Either<'retryLater', 'success'>>()

        return Promise.resolve(successResult)
      })
    })
  })
})
