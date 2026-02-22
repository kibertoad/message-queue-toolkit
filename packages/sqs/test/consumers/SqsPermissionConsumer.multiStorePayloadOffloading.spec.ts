import type { S3 } from '@aws-sdk/client-s3'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import {
  createMultiStoreConfig,
  type MultiPayloadStoreConfig,
  type SinglePayloadStoreConfig,
} from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

import { SQS_MESSAGE_MAX_SIZE } from '../../lib/sqs/AbstractSqsService.ts'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher.ts'
import { putObjectContent } from '../utils/s3Utils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

import { SqsPermissionConsumer } from './SqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SqsPermissionConsumer - multi-store payload offloading', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = SQS_MESSAGE_MAX_SIZE
    const s3BucketNameStore1 = 'test-bucket-store1'
    const s3BucketNameStore2 = 'test-bucket-store2'

    let diContainer: AwilixContainer<Dependencies>
    let s3: S3
    let testAdmin: TestAwsResourceAdmin

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      s3 = diContainer.cradle.s3
      testAdmin = diContainer.cradle.testAdmin

      await testAdmin.createBucket(s3BucketNameStore1)
      await testAdmin.createBucket(s3BucketNameStore2)
    })
    beforeEach(async () => {
      await testAdmin.deleteQueues(SqsPermissionConsumer.QUEUE_NAME)
    })
    afterEach(async () => {
      await publisher?.close()
      await consumer?.close(true)
    })
    afterAll(async () => {
      await testAdmin.emptyBuckets(s3BucketNameStore1, s3BucketNameStore2)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('multi-store consumer can read messages produced by multi-store publisher', async () => {
      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      const publisherConfig = createMultiStoreConfig({
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-west-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-eu-central-1',
      })

      const consumerConfig = createMultiStoreConfig({
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-west-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-us-west-1', // Consumer can have different outgoing store
      })

      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig: consumerConfig,
        deletionConfig: {
          deleteIfExists: false,
        },
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig: publisherConfig,
      })

      await consumer.start()
      await publisher.init()

      const message = {
        id: '2',
        messageType: 'add',
        metadata: {
          largeField: 'b'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Consumer should retrieve payload from 's3-eu-central-1' (specified in payloadRef)
      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
    })

    it('multi-store consumer can read messages produced by single-store publisher', async () => {
      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      const publisherConfig: SinglePayloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: store1,
        storeName: 's3-us-west-1',
      }

      const consumerConfig = createMultiStoreConfig({
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-west-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-us-west-1', // Consumer can have different outgoing store
      })

      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig: consumerConfig,
        deletionConfig: {
          deleteIfExists: false,
        },
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig: publisherConfig,
      })

      await consumer.start()
      await publisher.init()

      const message = {
        id: '2',
        messageType: 'add',
        metadata: {
          largeField: 'b'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Consumer should retrieve payload from 's3-us-west-1' (specified in payloadRef)
      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
    })

    it('single-store consumer can read messages produced by multi-store publisher', async () => {
      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      const publisherConfig = createMultiStoreConfig({
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-west-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-us-west-1',
      })

      const consumerConfig: SinglePayloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: store1,
        storeName: 's3-us-west-1',
      }

      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig: consumerConfig,
        deletionConfig: {
          deleteIfExists: false,
        },
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig: publisherConfig,
      })

      await consumer.start()
      await publisher.init()

      const message = {
        id: '2',
        messageType: 'add',
        metadata: {
          largeField: 'b'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Consumer should retrieve payload from 's3-us-west-1' (specified in payloadRef)
      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
    })

    // This test verifies that a multi-store consumer can read messages from older library versions
    // that only have offloadedPayloadPointer/offloadedPayloadSize (no payloadRef)
    it('multi-store consumer can read legacy messages (no payloadRef) using defaultIncomingStore', async () => {
      const TEST_QUEUE_NAME = 'user_permissions_legacy_default_store_test'
      const { sqsClient } = diContainer.cradle

      await testAdmin.deleteQueues(TEST_QUEUE_NAME)
      const { queueUrl } = await testAdmin.createQueue(TEST_QUEUE_NAME)

      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      // Manually create a payload in S3 (simulating what an old publisher would do)
      const originalMessage = {
        id: 'legacy-default-store-test',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        metadata: {
          largeField: 'c'.repeat(largeMessageSizeThreshold),
        },
      }
      const serializedPayload = JSON.stringify(originalMessage)
      const payloadKey = `legacy-payload-${Date.now()}`

      // Store payload in store1's bucket
      await putObjectContent(s3, s3BucketNameStore1, payloadKey, serializedPayload)

      // Send a message with ONLY legacy format (no payloadRef) - simulating old library version
      const legacyPointerMessage = {
        offloadedPayloadPointer: payloadKey,
        offloadedPayloadSize: serializedPayload.length,
        // Note: NO payloadRef field - this simulates a message from an older version
        id: originalMessage.id,
        messageType: originalMessage.messageType,
        timestamp: originalMessage.timestamp,
      }

      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(legacyPointerMessage),
          MessageAttributes: {
            [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]: {
              DataType: 'Number',
              StringValue: serializedPayload.length.toString(),
            },
          },
        }),
      )

      // Consumer uses multi-store config with defaultIncomingStore pointing to store1
      const consumerConfig = createMultiStoreConfig({
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-west-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-eu-central-1',
        defaultIncomingStore: 's3-us-west-1', // Legacy messages should be read from store1
      })

      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: { queueUrl },
        payloadStoreConfig: consumerConfig,
        deletionConfig: { deleteIfExists: false },
      })
      await consumer.start()

      // Consumer should be able to read legacy message using defaultIncomingStore
      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        originalMessage.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(originalMessage)

      await consumer.close(true)
    })

    it('multi-store consumer fails gracefully on legacy messages without defaultIncomingStore', async () => {
      const TEST_QUEUE_NAME = 'user_permissions_multi_store_no_default_test'
      const { sqsClient } = diContainer.cradle

      await testAdmin.deleteQueues(TEST_QUEUE_NAME)
      const { queueUrl } = await testAdmin.createQueue(TEST_QUEUE_NAME)

      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      // Manually create a payload in S3 (simulating what an old publisher would do)
      const originalMessage = {
        id: 'legacy-no-default-store-test',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        metadata: {
          largeField: 'd'.repeat(largeMessageSizeThreshold),
        },
      }
      const serializedPayload = JSON.stringify(originalMessage)
      const payloadKey = `legacy-payload-no-default-${Date.now()}`

      // Store payload in store1's bucket
      await putObjectContent(s3, s3BucketNameStore1, payloadKey, serializedPayload)

      // Send a message with ONLY legacy format (no payloadRef) - simulating old library version
      const legacyPointerMessage = {
        offloadedPayloadPointer: payloadKey,
        offloadedPayloadSize: serializedPayload.length,
        // Note: NO payloadRef field - this simulates a message from an older version
        id: originalMessage.id,
        messageType: originalMessage.messageType,
        timestamp: originalMessage.timestamp,
      }

      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(legacyPointerMessage),
          MessageAttributes: {
            [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]: {
              DataType: 'Number',
              StringValue: serializedPayload.length.toString(),
            },
          },
        }),
      )

      // Consumer uses multi-store config WITHOUT defaultIncomingStore
      const consumerConfig: MultiPayloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          'store-us': store1,
          'store-eu': store2,
        },
        outgoingStore: 'store-eu',
        // NO defaultIncomingStore specified
      }

      // Create a test error reporter to capture errors
      const capturedErrors: Array<{ error: Error; context?: Record<string, unknown> }> = []
      const testErrorReporter = {
        report: (errorData: { error: Error; context?: Record<string, unknown> }) => {
          capturedErrors.push(errorData)
        },
      }

      consumer = new SqsPermissionConsumer(
        { ...diContainer.cradle, errorReporter: testErrorReporter },
        {
          locatorConfig: { queueUrl },
          payloadStoreConfig: consumerConfig,
          deletionConfig: { deleteIfExists: false },
          consumerOverrides: {
            terminateVisibilityTimeout: true,
          },
        },
      )
      await consumer.start()

      // Wait for error to be reported
      await vi.waitFor(
        () => {
          const payloadError = capturedErrors.find((err) =>
            err.error.message.includes('defaultIncomingStore'),
          )
          expect(payloadError, 'Expected to find defaultIncomingStore error').toBeDefined()
        },
        { timeout: 10000, interval: 200 },
      )

      const payloadError = capturedErrors.find((err) =>
        err.error.message.includes('defaultIncomingStore'),
      )!
      expect(payloadError.error.message).toMatch(/defaultIncomingStore/)
      expect(payloadError.error.message).toMatch(/offloadedPayloadPointer/)

      await consumer.close(true)
    })
  })
})
