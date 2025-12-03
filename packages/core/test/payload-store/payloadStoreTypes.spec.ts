import { describe, expect, it } from 'vitest'

import {
  createMultiStoreConfig,
  isMultiPayloadStoreConfig,
  type MultiPayloadStoreConfig,
  type PayloadStore,
  type SinglePayloadStoreConfig,
} from '../../lib/payload-store/payloadStoreTypes.ts'

// Mock PayloadStore for testing
const mockStore: PayloadStore = {
  storePayload: async () => 'mock-key',
  retrievePayload: async () => null,
}

describe('payloadStoreTypes', () => {
  describe('isMultiPayloadStoreConfig', () => {
    it('returns true for multi-store config', () => {
      const config: MultiPayloadStoreConfig = {
        messageSizeThreshold: 1024,
        stores: {
          'store-a': mockStore,
          'store-b': mockStore,
        },
        outgoingStore: 'store-a',
      }

      expect(isMultiPayloadStoreConfig(config)).toBe(true)
    })

    it('returns true for multi-store config with defaultIncomingStore', () => {
      const config: MultiPayloadStoreConfig = {
        messageSizeThreshold: 1024,
        stores: {
          'store-a': mockStore,
          'store-b': mockStore,
        },
        outgoingStore: 'store-a',
        defaultIncomingStore: 'store-b',
      }

      expect(isMultiPayloadStoreConfig(config)).toBe(true)
    })

    it('returns false for single-store config', () => {
      const config: SinglePayloadStoreConfig = {
        messageSizeThreshold: 1024,
        store: mockStore,
      }

      expect(isMultiPayloadStoreConfig(config)).toBe(false)
    })

    it('returns false for single-store config with storeName', () => {
      const config: SinglePayloadStoreConfig = {
        messageSizeThreshold: 1024,
        store: mockStore,
        storeName: 'my-store',
      }

      expect(isMultiPayloadStoreConfig(config)).toBe(false)
    })
  })

  describe('createMultiStoreConfig', () => {
    it('returns the same config object', () => {
      const config: MultiPayloadStoreConfig = {
        messageSizeThreshold: 1024,
        stores: {
          'store-a': mockStore,
          'store-b': mockStore,
        },
        outgoingStore: 'store-a',
      }

      const result = createMultiStoreConfig(config)

      expect(result).toBe(config)
    })

    it('preserves all config properties', () => {
      const config = createMultiStoreConfig({
        messageSizeThreshold: 2048,
        stores: {
          primary: mockStore,
          secondary: mockStore,
        },
        outgoingStore: 'primary',
        defaultIncomingStore: 'secondary',
      })

      expect(config.messageSizeThreshold).toBe(2048)
      expect(config.outgoingStore).toBe('primary')
      expect(config.defaultIncomingStore).toBe('secondary')
      expect(Object.keys(config.stores)).toEqual(['primary', 'secondary'])
    })
  })
})
