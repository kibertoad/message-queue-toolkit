import type { S3 } from '@aws-sdk/client-s3'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { SinglePayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import {
  assertQueue,
  deleteQueue,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

import { SQS_MESSAGE_MAX_SIZE } from '../../lib/sqs/AbstractSqsService.ts'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher.ts'
import { assertBucket, emptyBucket, putObjectContent, waitForS3Objects } from '../utils/s3Utils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

import { SqsPermissionConsumer } from './SqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SqsPermissionConsumer - single-store payload offloading', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = SQS_MESSAGE_MAX_SIZE
    const s3BucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let s3: S3
    let payloadStoreConfig: SinglePayloadStoreConfig

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
        storeName: 's3',
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

    it('consumes message with legacy format (backward compatibility)', async () => {
      // This test verifies backward compatibility with messages that only have
      // offloadedPayloadPointer and offloadedPayloadSize (no payloadRef)
      const TEST_QUEUE_NAME = 'user_permissions_legacy_format_test'
      const { sqsClient } = diContainer.cradle

      await deleteQueue(sqsClient, TEST_QUEUE_NAME)
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: TEST_QUEUE_NAME })

      // Manually create a payload in S3 (simulating what an old publisher would do)
      const originalMessage = {
        id: 'legacy-format-test-1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        metadata: {
          largeField: 'd'.repeat(largeMessageSizeThreshold),
        },
      }
      const serializedPayload = JSON.stringify(originalMessage)
      const payloadKey = `legacy-test-payload-${Date.now()}`

      await putObjectContent(s3, s3BucketName, payloadKey, serializedPayload)

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
          // Include the size attribute so consumer knows this is an offloaded payload
          MessageAttributes: {
            [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]: {
              DataType: 'Number',
              StringValue: serializedPayload.length.toString(),
            },
          },
        }),
      )

      // Create consumer for the test queue
      const testConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: { queueUrl },
        payloadStoreConfig,
        deletionConfig: { deleteIfExists: false },
      })
      await testConsumer.start()

      // Consumer should be able to read the legacy format message
      const consumptionResult = await testConsumer.handlerSpy.waitForMessageWithId(
        originalMessage.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(originalMessage)

      await testConsumer.close(true)
    })
  })
})
