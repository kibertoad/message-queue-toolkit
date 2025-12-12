import { resolveMessageType } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'

import {
  BIGQUERY_TRANSFER_EVENT_TYPE_ATTRIBUTE,
  BIGQUERY_TRANSFER_EVENT_TYPES,
  BIGQUERY_TRANSFER_NORMALIZED_TYPE_RESOLVER,
  BIGQUERY_TRANSFER_TYPE_RESOLVER,
  CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER,
  CLOUD_EVENTS_TYPE_ATTRIBUTE,
  CLOUD_SCHEDULER_FUNCTION_TARGET_ATTRIBUTE,
  CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE,
  createAttributeResolver,
  createAttributeResolverWithMapping,
  createCloudSchedulerResolver,
  createCloudSchedulerResolverWithMapping,
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

  describe('BIGQUERY_TRANSFER_TYPE_RESOLVER', () => {
    it('extracts event type from BigQuery Data Transfer notifications', () => {
      const result = resolveMessageType(BIGQUERY_TRANSFER_TYPE_RESOLVER, {
        messageData: {
          name: 'projects/123/locations/us/transferConfigs/456/runs/789',
          state: 'SUCCEEDED',
        },
        messageAttributes: {
          [BIGQUERY_TRANSFER_EVENT_TYPE_ATTRIBUTE]:
            BIGQUERY_TRANSFER_EVENT_TYPES.TRANSFER_RUN_FINISHED,
          payloadFormat: 'JSON_API_V1',
        },
      })

      expect(result).toBe('TRANSFER_RUN_FINISHED')
    })

    it('throws when eventType attribute is missing', () => {
      expect(() =>
        resolveMessageType(BIGQUERY_TRANSFER_TYPE_RESOLVER, {
          messageData: { name: 'projects/123/runs/456', state: 'SUCCEEDED' },
          messageAttributes: { payloadFormat: 'JSON_API_V1' },
        }),
      ).toThrow(`attribute '${BIGQUERY_TRANSFER_EVENT_TYPE_ATTRIBUTE}' not found`)
    })
  })

  describe('BIGQUERY_TRANSFER_NORMALIZED_TYPE_RESOLVER', () => {
    it('maps TRANSFER_RUN_FINISHED to normalized type', () => {
      const result = resolveMessageType(BIGQUERY_TRANSFER_NORMALIZED_TYPE_RESOLVER, {
        messageData: { name: 'projects/123/runs/456', state: 'SUCCEEDED' },
        messageAttributes: {
          [BIGQUERY_TRANSFER_EVENT_TYPE_ATTRIBUTE]:
            BIGQUERY_TRANSFER_EVENT_TYPES.TRANSFER_RUN_FINISHED,
        },
      })

      expect(result).toBe('bigquery.transfer.finished')
    })

    it('falls back to original type for unknown events', () => {
      const result = resolveMessageType(BIGQUERY_TRANSFER_NORMALIZED_TYPE_RESOLVER, {
        messageData: { name: 'projects/123/runs/456' },
        messageAttributes: {
          [BIGQUERY_TRANSFER_EVENT_TYPE_ATTRIBUTE]: 'UNKNOWN_BIGQUERY_EVENT',
        },
      })

      expect(result).toBe('UNKNOWN_BIGQUERY_EVENT')
    })
  })

  describe('createCloudSchedulerResolver', () => {
    it('creates resolver using default jobType attribute', () => {
      const resolver = createCloudSchedulerResolver()

      const result = resolveMessageType(resolver, {
        messageData: { task: 'generate-report' },
        messageAttributes: {
          [CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE]: 'daily-report',
        },
      })

      expect(result).toBe('daily-report')
    })

    it('creates resolver using custom attribute name', () => {
      const resolver = createCloudSchedulerResolver(CLOUD_SCHEDULER_FUNCTION_TARGET_ATTRIBUTE)

      const result = resolveMessageType(resolver, {
        messageData: { task: 'cleanup' },
        messageAttributes: {
          [CLOUD_SCHEDULER_FUNCTION_TARGET_ATTRIBUTE]: 'weekly-cleanup',
        },
      })

      expect(result).toBe('weekly-cleanup')
    })

    it('throws when attribute is missing', () => {
      const resolver = createCloudSchedulerResolver()

      expect(() =>
        resolveMessageType(resolver, {
          messageData: { task: 'generate-report' },
          messageAttributes: {},
        }),
      ).toThrow(`attribute '${CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE}' not found`)
    })
  })

  describe('createCloudSchedulerResolverWithMapping', () => {
    it('maps job types to internal message types', () => {
      const resolver = createCloudSchedulerResolverWithMapping({
        'daily-report': 'scheduler.report.daily',
        'weekly-cleanup': 'scheduler.cleanup.weekly',
      })

      const result = resolveMessageType(resolver, {
        messageData: {},
        messageAttributes: {
          [CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE]: 'daily-report',
        },
      })

      expect(result).toBe('scheduler.report.daily')
    })

    it('uses custom attribute name', () => {
      const resolver = createCloudSchedulerResolverWithMapping(
        { 'process-orders': 'scheduler.orders.process' },
        'taskType',
      )

      const result = resolveMessageType(resolver, {
        messageData: {},
        messageAttributes: { taskType: 'process-orders' },
      })

      expect(result).toBe('scheduler.orders.process')
    })

    it('throws when value is not mapped and fallback is disabled', () => {
      const resolver = createCloudSchedulerResolverWithMapping({
        'daily-report': 'scheduler.report.daily',
      })

      expect(() =>
        resolveMessageType(resolver, {
          messageData: {},
          messageAttributes: {
            [CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE]: 'unknown-job',
          },
        }),
      ).toThrow("'unknown-job' is not mapped")
    })

    it('falls back to original value when fallbackToOriginal is true', () => {
      const resolver = createCloudSchedulerResolverWithMapping(
        { 'daily-report': 'scheduler.report.daily' },
        CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE,
        { fallbackToOriginal: true },
      )

      const result = resolveMessageType(resolver, {
        messageData: {},
        messageAttributes: {
          [CLOUD_SCHEDULER_JOB_TYPE_ATTRIBUTE]: 'unmapped-job',
        },
      })

      expect(result).toBe('unmapped-job')
    })
  })
})
