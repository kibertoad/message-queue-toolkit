import { S3 } from '@aws-sdk/client-s3'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { S3PayloadStore } from '../../lib/S3PayloadStore'
import {
  assertBucket,
  deleteBucketWithObjects,
  getObjectContent,
  objectExists,
} from '../utils/s3Utils'
import { TEST_AWS_CONFIG } from '../utils/testS3Config'

const TEST_BUCKET = 'test-bucket'

describe('S3PayloadStore', () => {
  let s3: S3
  let store: S3PayloadStore

  beforeAll(async () => {
    s3 = new S3(TEST_AWS_CONFIG)
    store = new S3PayloadStore({ s3 }, { bucketName: TEST_BUCKET, keyPrefix: 'test' })
  })
  beforeEach(async () => {
    await assertBucket(s3, TEST_BUCKET)
  })
  afterEach(async () => {
    await deleteBucketWithObjects(s3, TEST_BUCKET)
  })

  describe('storePayload', () => {
    it('stores the payload in the bucket', async () => {
      const payload = 'test'

      const key = await store.storePayload(payload)

      expect(key).toContain('test/')
      const result = await getObjectContent(s3, TEST_BUCKET, key)
      expect(result).toBe(payload)
    })
  })
  describe('retrievePayload', () => {
    it('retrieves previously stored payload', async () => {
      const payload = 'test'
      const key = await store.storePayload(payload)

      const result = await store.retrievePayload(key)

      expect(result).toBe(payload)
    })
    it('returns null if payload cannot be found', async () => {
      const result = await store.retrievePayload('non-existing-key')
      expect(result).toBe(null)
    })
  })
  describe('deletePayload', () => {
    it('successfully deletes previously stored payload', async () => {
      const key = await store.storePayload('test')
      await expect(objectExists(s3, TEST_BUCKET, key)).resolves.toBeTruthy()

      await store.deletePayload(key)

      await expect(objectExists(s3, TEST_BUCKET, key)).resolves.toBeFalsy()
    })
    it('gracefully handles non-existing key', async () => {
      await expect(store.deletePayload('non-existing-key')).resolves.not.toThrow()
    })
  })
})
