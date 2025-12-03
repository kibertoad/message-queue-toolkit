import { describe, expect, it } from 'vitest'

import {
  isOffloadedPayloadPointerPayload,
  OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA,
} from '../../lib/payload-store/offloadedPayloadMessageSchemas.ts'

describe('offloadedPayloadMessageSchemas', () => {
  describe('OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA', () => {
    describe('new format (payloadRef only)', () => {
      it('parses message with only payloadRef', () => {
        const message = {
          payloadRef: {
            id: 'payload-123',
            store: 'my-store',
            size: 2048,
          },
          id: 'message-1',
          messageType: 'test',
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(true)
        expect(result.data).toMatchObject({
          payloadRef: {
            id: 'payload-123',
            store: 'my-store',
            size: 2048,
          },
          id: 'message-1',
          messageType: 'test',
        })
      })
    })

    describe('legacy format (offloadedPayloadPointer only)', () => {
      it('parses message with only legacy fields', () => {
        const message = {
          offloadedPayloadPointer: 'legacy-pointer-123',
          offloadedPayloadSize: 4096,
          id: 'message-2',
          messageType: 'test',
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(true)
        expect(result.data).toMatchObject({
          offloadedPayloadPointer: 'legacy-pointer-123',
          offloadedPayloadSize: 4096,
          id: 'message-2',
          messageType: 'test',
        })
      })
    })

    describe('combined format (both payloadRef and legacy fields)', () => {
      it('parses message with both new and legacy fields', () => {
        const message = {
          payloadRef: {
            id: 'payload-456',
            store: 'store-eu',
            size: 8192,
          },
          offloadedPayloadPointer: 'payload-456',
          offloadedPayloadSize: 8192,
          id: 'message-3',
          messageType: 'test',
          timestamp: '2024-01-01T00:00:00Z',
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(true)
        expect(result.data).toMatchObject({
          payloadRef: {
            id: 'payload-456',
            store: 'store-eu',
            size: 8192,
          },
          offloadedPayloadPointer: 'payload-456',
          offloadedPayloadSize: 8192,
        })
      })
    })

    describe('invalid cases', () => {
      it('rejects message with neither payloadRef nor offloadedPayloadPointer', () => {
        const message = {
          id: 'message-4',
          messageType: 'test',
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(false)
      })

      it('rejects message with only offloadedPayloadSize (no pointer)', () => {
        const message = {
          offloadedPayloadSize: 1024,
          id: 'message-5',
          messageType: 'test',
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(false)
      })
    })

    describe('passthrough behavior', () => {
      it('preserves additional fields', () => {
        const message = {
          payloadRef: {
            id: 'payload-789',
            store: 'store-us',
            size: 1024,
          },
          id: 'message-6',
          messageType: 'custom-type',
          customField: 'custom-value',
          nestedObject: { key: 'value' },
        }

        const result = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(message)

        expect(result.success).toBe(true)
        expect(result.data).toMatchObject({
          customField: 'custom-value',
          nestedObject: { key: 'value' },
        })
      })
    })
  })

  describe('isOffloadedPayloadPointerPayload', () => {
    it('returns true for message with payloadRef', () => {
      const message = {
        payloadRef: {
          id: 'payload-123',
          store: 'my-store',
          size: 1024,
        },
      }

      expect(isOffloadedPayloadPointerPayload(message)).toBe(true)
    })

    it('returns true for message with offloadedPayloadPointer', () => {
      const message = {
        offloadedPayloadPointer: 'legacy-pointer',
        offloadedPayloadSize: 2048,
      }

      expect(isOffloadedPayloadPointerPayload(message)).toBe(true)
    })

    it('returns true for message with both formats', () => {
      const message = {
        payloadRef: {
          id: 'payload-456',
          store: 'store-eu',
          size: 4096,
        },
        offloadedPayloadPointer: 'payload-456',
        offloadedPayloadSize: 4096,
      }

      expect(isOffloadedPayloadPointerPayload(message)).toBe(true)
    })

    it('returns false for non valid values', () => {
      // Regular message without pointer
      expect(
        isOffloadedPayloadPointerPayload({
          id: 'regular-message',
          messageType: 'test',
          data: { foo: 'bar' },
        }),
      ).toBe(false)

      // Null, undefined, and primitive types
      expect(isOffloadedPayloadPointerPayload(null)).toBe(false)
      expect(isOffloadedPayloadPointerPayload(undefined)).toBe(false)
      expect(isOffloadedPayloadPointerPayload('string')).toBe(false)
      expect(isOffloadedPayloadPointerPayload(123)).toBe(false)
      expect(isOffloadedPayloadPointerPayload(true)).toBe(false)
    })
  })
})
