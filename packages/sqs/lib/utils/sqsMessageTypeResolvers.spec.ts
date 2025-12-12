import { resolveMessageType } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'

import {
  createEventBridgeResolverWithMapping,
  EVENT_BRIDGE_DETAIL_TYPE_FIELD,
  EVENT_BRIDGE_TYPE_RESOLVER,
} from './sqsMessageTypeResolvers.ts'

describe('sqsMessageTypeResolvers', () => {
  describe('EVENT_BRIDGE_TYPE_RESOLVER', () => {
    it('extracts type from detail-type field', () => {
      const eventBridgeEvent = {
        version: '0',
        id: '12345678-1234-1234-1234-123456789012',
        'detail-type': 'Order Created',
        source: 'com.myapp.orders',
        account: '123456789012',
        time: '2024-01-15T10:30:00Z',
        region: 'us-east-1',
        resources: [],
        detail: {
          orderId: 'order-456',
          amount: 99.99,
        },
      }

      const result = resolveMessageType(EVENT_BRIDGE_TYPE_RESOLVER, {
        messageData: eventBridgeEvent,
      })

      expect(result).toBe('Order Created')
    })

    it('works with different detail-type values', () => {
      const events = [
        { 'detail-type': 'User Signed Up', expected: 'User Signed Up' },
        { 'detail-type': 'payment.completed', expected: 'payment.completed' },
        {
          'detail-type': 'EC2 Instance State-change Notification',
          expected: 'EC2 Instance State-change Notification',
        },
      ]

      for (const { 'detail-type': detailType, expected } of events) {
        const result = resolveMessageType(EVENT_BRIDGE_TYPE_RESOLVER, {
          messageData: { [EVENT_BRIDGE_DETAIL_TYPE_FIELD]: detailType },
        })
        expect(result).toBe(expected)
      }
    })

    it('throws when detail-type field is missing', () => {
      expect(() =>
        resolveMessageType(EVENT_BRIDGE_TYPE_RESOLVER, {
          messageData: { source: 'com.myapp.orders', detail: {} },
        }),
      ).toThrow("path 'detail-type' not found")
    })
  })

  describe('createEventBridgeResolverWithMapping', () => {
    it('maps detail-type values to internal types', () => {
      const resolver = createEventBridgeResolverWithMapping({
        'Order Created': 'order.created',
        'Order Updated': 'order.updated',
        'Order Cancelled': 'order.cancelled',
      })

      const eventBridgeEvent = {
        'detail-type': 'Order Created',
        source: 'com.myapp.orders',
        detail: { orderId: '123' },
      }

      const result = resolveMessageType(resolver, {
        messageData: eventBridgeEvent,
      })

      expect(result).toBe('order.created')
    })

    it('throws when detail-type is not mapped and fallback is disabled', () => {
      const resolver = createEventBridgeResolverWithMapping({
        'Order Created': 'order.created',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { 'detail-type': 'Unknown Event' },
        }),
      ).toThrow("'Unknown Event' is not mapped")
    })

    it('falls back to original value when fallbackToOriginal is true', () => {
      const resolver = createEventBridgeResolverWithMapping(
        { 'Order Created': 'order.created' },
        { fallbackToOriginal: true },
      )

      const result = resolveMessageType(resolver, {
        messageData: { 'detail-type': 'Unknown Event' },
      })

      expect(result).toBe('Unknown Event')
    })

    it('throws when detail-type is missing', () => {
      const resolver = createEventBridgeResolverWithMapping({
        'Order Created': 'order.created',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { source: 'com.myapp', detail: {} },
        }),
      ).toThrow("'detail-type' field not found")
    })

    it('handles null detail-type value', () => {
      const resolver = createEventBridgeResolverWithMapping({
        'Order Created': 'order.created',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { 'detail-type': null },
        }),
      ).toThrow("'detail-type' field not found")
    })
  })
})
