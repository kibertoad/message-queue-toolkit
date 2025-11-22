import { Readable } from 'node:stream'

import type { Storage } from '@google-cloud/storage'
import { beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { GCSPayloadStore, resolvePayloadStoreConfig } from '../../lib/GCSPayloadStore.ts'
import { assertEmptyBucket, getObjectContent, objectExists } from '../utils/gcsUtils.ts'
import { streamToString } from '../utils/streamUtils.ts'
import { createTestGCSClient } from '../utils/testGCSConfig.ts'

const TEST_BUCKET = 'test-bucket'

describe('GCSPayloadStore', () => {
  let storage: Storage
  let store: GCSPayloadStore

  beforeAll(() => {
    storage = createTestGCSClient()
    store = new GCSPayloadStore({ gcsStorage: storage }, { bucketName: TEST_BUCKET })
  })

  beforeEach(async () => {
    await assertEmptyBucket(storage, TEST_BUCKET)
  })

  describe('storePayload', () => {
    it('stores string payload in the bucket', async () => {
      const payload = 'test'

      const stringPayloadKey = await store.storePayload({
        value: payload,
        size: payload.length,
      })

      expect(await getObjectContent(storage, TEST_BUCKET, stringPayloadKey)).toBe(payload)
    })

    it('stores stream payload in the bucket', async () => {
      const payload = 'test stream content'

      const streamPayloadKey = await store.storePayload({
        value: Readable.from(payload),
        size: payload.length,
      })

      expect(await getObjectContent(storage, TEST_BUCKET, streamPayloadKey)).toBe(payload)
    })

    it('uses key prefix if provided', async () => {
      const prefixedStore = new GCSPayloadStore(
        { gcsStorage: storage },
        { bucketName: TEST_BUCKET, keyPrefix: 'prefix' },
      )
      const payload = 'test'

      const stringPayloadKey = await prefixedStore.storePayload({
        value: payload,
        size: payload.length,
      })

      expect(stringPayloadKey).toContain('prefix/')
      expect(await getObjectContent(storage, TEST_BUCKET, stringPayloadKey)).toBe(payload)
    })
  })

  describe('retrievePayload', () => {
    it('retrieves previously stored payload', async () => {
      const payload = 'test retrieval content'
      const key = await store.storePayload({
        value: Readable.from(payload),
        size: payload.length,
      })

      const result = await store.retrievePayload(key)

      expect(result).toBeInstanceOf(Readable)
      await expect(streamToString(result!)).resolves.toBe(payload)
    })

    it('returns null if payload cannot be found', async () => {
      const result = await store.retrievePayload('non-existing-key')
      expect(result).toBe(null)
    })

    it('throws if other than not-found error occurs', async () => {
      const invalidStore = new GCSPayloadStore(
        { gcsStorage: storage },
        { bucketName: 'non-existing-bucket' },
      )
      await expect(invalidStore.retrievePayload('some-key')).rejects.toThrow()
    })
  })

  describe('deletePayload', () => {
    it('successfully deletes previously stored payload', async () => {
      const payload = 'test deletion content'
      const key = await store.storePayload({
        value: Readable.from(payload),
        size: payload.length,
      })
      await expect(objectExists(storage, TEST_BUCKET, key)).resolves.toBeTruthy()

      await store.deletePayload(key)

      await expect(objectExists(storage, TEST_BUCKET, key)).resolves.toBeFalsy()
    })

    it('gracefully handles non-existing key', async () => {
      await expect(store.deletePayload('non-existing-key')).resolves.not.toThrow()
    })
  })

  describe('resolvePayloadStoreConfig', () => {
    it('should return undefined if gcsPayloadOffloadingBucket is not set', () => {
      const result = resolvePayloadStoreConfig({ gcsStorage: {} as any })
      expect(result).toBeUndefined()
    })

    it('should throw an error if GCS storage client is not defined', () => {
      expect(() =>
        resolvePayloadStoreConfig(
          { gcsStorage: undefined },
          {
            gcsPayloadOffloadingBucket: 'test-bucket',
            messageSizeThreshold: 1,
          },
        ),
      ).toThrowError('Google Cloud Storage client is required for payload offloading')
    })

    it('should return payload store config', () => {
      const result = resolvePayloadStoreConfig(
        { gcsStorage: {} as any },
        {
          gcsPayloadOffloadingBucket: 'test-bucket',
          messageSizeThreshold: 1,
        },
      )
      expect(result).toEqual({
        store: expect.any(GCSPayloadStore),
        messageSizeThreshold: 1,
      })
    })
  })
})
