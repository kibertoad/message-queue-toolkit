import type { S3 } from '@aws-sdk/client-s3'
import type { PayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import { deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

import { SQS_MESSAGE_MAX_SIZE } from '../../lib/sqs/AbstractSqsService.ts'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher.ts'
import { assertBucket, emptyBucket, waitForS3Objects } from '../utils/s3Utils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

import { SqsPermissionConsumer } from './SqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SqsPermissionConsumer', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = SQS_MESSAGE_MAX_SIZE
    const s3BucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let s3: S3
    let payloadStoreConfig: PayloadStoreConfig

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      s3 = diContainer.cradle.s3

      await assertBucket(s3, s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
      }
    })
    beforeEach(async () => {
      const { sqsClient } = diContainer.cradle
      await deleteQueue(sqsClient, SqsPermissionConsumer.QUEUE_NAME)

      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig,
        deletionConfig: {
          deleteIfExists: false, // Don't delete queue when closing - we'll reuse it
        },
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig,
      })
      await consumer.start()
      await publisher.init()
    })
    afterEach(async () => {
      await publisher.close()
      await consumer.close(true)
    })
    afterAll(async () => {
      await emptyBucket(s3, s3BucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('consumes large message with offloaded payload', async () => {
      // Craft a message that is larger than the max message size
      const message = {
        id: '1',
        messageType: 'add',
        metadata: {
          largeField: 'a'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

      await publisher.publish(message)

      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
    })

    it('handles missing offloaded payload gracefully', async () => {
      // Use completely isolated queue to avoid conflicts with other tests
      const TEST_QUEUE_NAME = 'user_permissions_offloading_error_test'
      const { sqsClient } = diContainer.cradle

      // Clean up any existing test queue
      await deleteQueue(sqsClient, TEST_QUEUE_NAME)

      // Create dedicated publisher with isolated queue
      const testPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: TEST_QUEUE_NAME,
          },
        },
        payloadStoreConfig,
        deletionConfig: {
          deleteIfExists: true,
        },
      })
      await testPublisher.init()

      // Craft a message that is larger than the max message size
      const message = {
        id: '2',
        messageType: 'add',
        metadata: {
          largeField: 'b'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

      // Publish message - this offloads payload to S3
      await testPublisher.publish(message)

      // Wait for publisher to confirm and S3 object to be created
      await testPublisher.handlerSpy.waitForMessageWithId(message.id, 'published')
      const s3Keys = await waitForS3Objects(s3, s3BucketName, 1, 5000)
      expect(s3Keys.length).toBeGreaterThan(0)

      // Close publisher before deleting S3 object
      await testPublisher.close()

      // Delete the S3 object to simulate S3 failure BEFORE starting consumer
      await emptyBucket(s3, s3BucketName)

      // Create a test error reporter to capture deserialization errors
      const capturedErrors: Array<{ error: Error; context?: Record<string, unknown> }> = []
      const testErrorReporter = {
        report: (errorData: { error: Error; context?: Record<string, unknown> }) => {
          capturedErrors.push(errorData)
        },
      }

      // Create dedicated consumer with isolated queue and test error reporter
      const testConsumer = new SqsPermissionConsumer(
        { ...diContainer.cradle, errorReporter: testErrorReporter },
        {
          locatorConfig: {
            queueUrl: testPublisher.queueProps.url,
          },
          payloadStoreConfig,
          deletionConfig: {
            deleteIfExists: false,
          },
          consumerOverrides: {
            terminateVisibilityTimeout: true,
          },
        },
      )
      await testConsumer.start()

      // Wait deterministically for error to be reported
      await vi.waitFor(
        () => {
          const payloadError = capturedErrors.find((err) =>
            err.error.message.includes('was not found in the store'),
          )
          expect(payloadError, 'Expected to find payload retrieval error').toBeDefined()
        },
        { timeout: 10000, interval: 200 },
      )

      const payloadError = capturedErrors.find((err) =>
        err.error.message.includes('was not found in the store'),
      )!
      expect(payloadError.error.message).toMatch(/was not found in the store/)

      // Clean up
      await testConsumer.close(true)
    })
  })
})
