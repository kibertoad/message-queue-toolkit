import { resolveMessageType } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'

import {
  CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER,
  CLOUD_EVENTS_TYPE_ATTRIBUTE,
  createAttributeResolver,
  createAttributeResolverWithMapping,
  GCS_EVENT_TYPE_ATTRIBUTE,
  GCS_EVENT_TYPES,
  GCS_NOTIFICATION_RAW_TYPE_RESOLVER,
  GCS_NOTIFICATION_TYPE_RESOLVER,
} from './gcpMessageTypeResolvers.ts'

describe('gcpMessageTypeResolvers', () => {
  describe('createAttributeResolver', () => {
    it('extracts type from message attribute', () => {
      const resolver = createAttributeResolver('eventType')

      const result = resolveMessageType(resolver, {
        messageData: { id: '123' },
        messageAttributes: { eventType: 'OBJECT_FINALIZE' },
      })

      expect(result).toBe('OBJECT_FINALIZE')
    })

    it('throws when attribute is missing', () => {
      const resolver = createAttributeResolver('eventType')

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { id: '123' },
          messageAttributes: {},
        }),
      ).toThrow("attribute 'eventType' not found")
    })

    it('throws when attributes are undefined', () => {
      const resolver = createAttributeResolver('eventType')

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { id: '123' },
          messageAttributes: undefined,
        }),
      ).toThrow("attribute 'eventType' not found")
    })

    it('handles null attribute value', () => {
      const resolver = createAttributeResolver('eventType')

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { id: '123' },
          messageAttributes: { eventType: null },
        }),
      ).toThrow("attribute 'eventType' not found")
    })
  })

  describe('createAttributeResolverWithMapping', () => {
    it('maps attribute value to internal type', () => {
      const resolver = createAttributeResolverWithMapping('eventType', {
        OBJECT_FINALIZE: 'storage.object.created',
        OBJECT_DELETE: 'storage.object.deleted',
      })

      const result = resolveMessageType(resolver, {
        messageData: { id: '123' },
        messageAttributes: { eventType: 'OBJECT_FINALIZE' },
      })

      expect(result).toBe('storage.object.created')
    })

    it('throws when value is not mapped and fallback is disabled', () => {
      const resolver = createAttributeResolverWithMapping('eventType', {
        OBJECT_FINALIZE: 'storage.object.created',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { id: '123' },
          messageAttributes: { eventType: 'UNKNOWN_EVENT' },
        }),
      ).toThrow("'UNKNOWN_EVENT' is not mapped")
    })

    it('falls back to original value when fallbackToOriginal is true', () => {
      const resolver = createAttributeResolverWithMapping(
        'eventType',
        { OBJECT_FINALIZE: 'storage.object.created' },
        { fallbackToOriginal: true },
      )

      const result = resolveMessageType(resolver, {
        messageData: { id: '123' },
        messageAttributes: { eventType: 'UNKNOWN_EVENT' },
      })

      expect(result).toBe('UNKNOWN_EVENT')
    })

    it('throws when attribute is missing', () => {
      const resolver = createAttributeResolverWithMapping('eventType', {
        OBJECT_FINALIZE: 'storage.object.created',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { id: '123' },
          messageAttributes: {},
        }),
      ).toThrow("attribute 'eventType' not found")
    })
  })

  describe('CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER', () => {
    it('extracts type from ce-type attribute', () => {
      const result = resolveMessageType(CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER, {
        messageData: { id: '123', data: { orderId: '456' } },
        messageAttributes: {
          [CLOUD_EVENTS_TYPE_ATTRIBUTE]: 'com.example.order.created',
          'ce-source': 'https://example.com/orders',
          'ce-specversion': '1.0',
        },
      })

      expect(result).toBe('com.example.order.created')
    })

    it('throws when ce-type attribute is missing', () => {
      expect(() =>
        resolveMessageType(CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER, {
          messageData: { id: '123' },
          messageAttributes: {
            'ce-source': 'https://example.com/orders',
          },
        }),
      ).toThrow(`attribute '${CLOUD_EVENTS_TYPE_ATTRIBUTE}' not found`)
    })
  })

  describe('GCS_NOTIFICATION_TYPE_RESOLVER', () => {
    it('maps GCS event types to normalized internal types', () => {
      const testCases = [
        { gcsType: GCS_EVENT_TYPES.OBJECT_FINALIZE, expected: 'gcs.object.finalized' },
        { gcsType: GCS_EVENT_TYPES.OBJECT_DELETE, expected: 'gcs.object.deleted' },
        { gcsType: GCS_EVENT_TYPES.OBJECT_ARCHIVE, expected: 'gcs.object.archived' },
        { gcsType: GCS_EVENT_TYPES.OBJECT_METADATA_UPDATE, expected: 'gcs.object.metadataUpdated' },
      ]

      for (const { gcsType, expected } of testCases) {
        const result = resolveMessageType(GCS_NOTIFICATION_TYPE_RESOLVER, {
          messageData: { kind: 'storage#object', bucket: 'my-bucket' },
          messageAttributes: {
            [GCS_EVENT_TYPE_ATTRIBUTE]: gcsType,
            bucketId: 'my-bucket',
            objectId: 'path/to/file.jpg',
          },
        })

        expect(result).toBe(expected)
      }
    })

    it('falls back to original type for unknown GCS events', () => {
      const result = resolveMessageType(GCS_NOTIFICATION_TYPE_RESOLVER, {
        messageData: { kind: 'storage#object' },
        messageAttributes: {
          [GCS_EVENT_TYPE_ATTRIBUTE]: 'UNKNOWN_GCS_EVENT',
        },
      })

      expect(result).toBe('UNKNOWN_GCS_EVENT')
    })

    it('throws when eventType attribute is missing', () => {
      expect(() =>
        resolveMessageType(GCS_NOTIFICATION_TYPE_RESOLVER, {
          messageData: { kind: 'storage#object' },
          messageAttributes: { bucketId: 'my-bucket' },
        }),
      ).toThrow(`attribute '${GCS_EVENT_TYPE_ATTRIBUTE}' not found`)
    })
  })

  describe('GCS_NOTIFICATION_RAW_TYPE_RESOLVER', () => {
    it('returns raw GCS event type without mapping', () => {
      const result = resolveMessageType(GCS_NOTIFICATION_RAW_TYPE_RESOLVER, {
        messageData: { kind: 'storage#object' },
        messageAttributes: {
          [GCS_EVENT_TYPE_ATTRIBUTE]: 'OBJECT_FINALIZE',
        },
      })

      expect(result).toBe('OBJECT_FINALIZE')
    })
  })
})
