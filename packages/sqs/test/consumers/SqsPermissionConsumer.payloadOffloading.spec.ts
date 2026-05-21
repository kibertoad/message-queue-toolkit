import type { S3 } from '@aws-sdk/client-s3'
import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs'
import type { SinglePayloadStoreConfig } from '@message-queue-toolkit/core'
import { MessageCodecEnum, MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import z from 'zod/v4'

import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsPublisher } from '../../lib/sqs/AbstractSqsPublisher.ts'
import { SQS_MESSAGE_MAX_SIZE } from '../../lib/sqs/AbstractSqsService.ts'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher.ts'
import { getObjectBuffer, putObjectContent, waitForS3Objects } from '../utils/s3Utils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
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
    let testAdmin: TestAwsResourceAdmin
    let payloadStoreConfig: SinglePayloadStoreConfig

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      s3 = diContainer.cradle.s3
      testAdmin = diContainer.cradle.testAdmin

      await testAdmin.createBucket(s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
        storeName: 's3',
      }
    })
    beforeEach(async () => {
      await testAdmin.deleteQueues(SqsPermissionConsumer.QUEUE_NAME)

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
      await testAdmin.emptyBuckets(s3BucketName)

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

      // Clean up any existing test queue
      await testAdmin.deleteQueues(TEST_QUEUE_NAME)

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
      await testAdmin.emptyBuckets(s3BucketName)

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

      await testAdmin.deleteQueues(TEST_QUEUE_NAME)
      const { queueUrl } = await testAdmin.createQueue(TEST_QUEUE_NAME)

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

describe('SqsPermissionConsumer - nested messageTypePath with payload offloading', () => {
  /**
   * Tests that nested messageTypePath (e.g., 'metadata.type') is correctly preserved
   * when payload is offloaded. The type field at nested path should be maintained
   * in the pointer message so routing still works correctly.
   */
  describe('consume with nested type path', () => {
    const largeMessageSizeThreshold = SQS_MESSAGE_MAX_SIZE
    const s3BucketName = 'test-bucket-nested-path'
    const TEST_QUEUE_NAME = 'nested_type_path_offloading_test'

    let diContainer: AwilixContainer<Dependencies>
    let testAdmin: TestAwsResourceAdmin

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      testAdmin = diContainer.cradle.testAdmin

      await testAdmin.createBucket(s3BucketName)
    })

    afterAll(async () => {
      await testAdmin.emptyBuckets(s3BucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('preserves nested messageTypePath when offloading payload', async () => {
      await testAdmin.deleteQueues(TEST_QUEUE_NAME)

      const payloadStoreConfig: SinglePayloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
        storeName: 's3',
      }

      // Schema with nested type path at 'metadata.type'
      const nestedTypeSchema = z.object({
        id: z.string(),
        metadata: z.object({
          type: z.literal('nested.event'),
          largeField: z.string().optional(),
        }),
        timestamp: z.string().optional(),
      })

      type NestedTypeMessage = z.output<typeof nestedTypeSchema>
      type ExecutionContext = Record<string, never>

      // Create publisher with nested messageTypePath
      class NestedPathPublisher extends AbstractSqsPublisher<NestedTypeMessage> {
        constructor(deps: Dependencies) {
          super(deps, {
            creationConfig: {
              queue: { QueueName: TEST_QUEUE_NAME },
            },
            messageSchemas: [nestedTypeSchema],
            messageTypeResolver: { messageTypePath: 'metadata.type' },
            handlerSpy: true,
            payloadStoreConfig,
            deletionConfig: { deleteIfExists: true },
          })
        }
      }

      let receivedMessage: NestedTypeMessage | null = null

      // Initialize publisher first to create queue
      const publisher = new NestedPathPublisher(diContainer.cradle)
      await publisher.init()

      // Create large message with nested type
      const message: NestedTypeMessage = {
        id: 'nested-path-test-1',
        metadata: {
          type: 'nested.event',
          largeField: 'x'.repeat(largeMessageSizeThreshold),
        },
      }
      expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

      // Publish - should offload to S3 but preserve metadata.type
      await publisher.publish(message)
      await publisher.handlerSpy.waitForMessageWithId(message.id, 'published')

      // Create consumer pointing to the queue
      // @ts-expect-error - accessing protected property for test
      const queueUrl = publisher.queueUrl

      class NestedPathConsumer extends AbstractSqsConsumer<NestedTypeMessage, ExecutionContext> {
        constructor(deps: Dependencies) {
          super(
            deps,
            {
              locatorConfig: { queueUrl },
              messageTypeResolver: { messageTypePath: 'metadata.type' },
              handlerSpy: true,
              payloadStoreConfig,
              deletionConfig: { deleteIfExists: false },
              consumerOverrides: { terminateVisibilityTimeout: true },
              handlers: new MessageHandlerConfigBuilder<NestedTypeMessage, ExecutionContext>()
                .addConfig(nestedTypeSchema, (msg) => {
                  receivedMessage = msg
                  return Promise.resolve({ result: 'success' })
                })
                .build(),
            },
            {},
          )
        }
      }

      const consumer = new NestedPathConsumer(diContainer.cradle)
      await consumer.start()

      // Wait for message to be consumed
      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )

      // Verify the message was consumed correctly with nested type preserved
      expect(consumptionResult.message).toMatchObject({
        id: 'nested-path-test-1',
        metadata: {
          type: 'nested.event',
          largeField: 'x'.repeat(largeMessageSizeThreshold),
        },
      })
      expect(receivedMessage).toMatchObject({
        id: 'nested-path-test-1',
        metadata: {
          type: 'nested.event',
        },
      })

      // Clean up
      await consumer.close(true)
      await publisher.close()
    })
  })
})

describe('SqsPermissionConsumer - codec + payload offloading', () => {
  const s3BucketName = 'test-bucket-codec'
  // Threshold low enough that even a small compressed payload triggers offloading
  const smallThreshold = 10

  let diContainer: AwilixContainer<Dependencies>
  let s3: S3
  let testAdmin: TestAwsResourceAdmin
  let payloadStoreConfig: SinglePayloadStoreConfig

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    s3 = diContainer.cradle.s3
    testAdmin = diContainer.cradle.testAdmin

    await testAdmin.createBucket(s3BucketName)
    payloadStoreConfig = {
      messageSizeThreshold: smallThreshold,
      store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
      storeName: 's3',
    }
  })

  afterAll(async () => {
    await testAdmin.emptyBuckets(s3BucketName)
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  it('S3 object is raw zstd binary and SQS message carries a plain pointer (not a codec envelope)', async () => {
    // Use an isolated queue with no consumer so we can read the raw SQS message without a race
    const wireQueueName = 'codec-offload-wire-check'
    await testAdmin.deleteQueues(wireQueueName)

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-offload-wire-1',
      messageType: 'add',
      metadata: { info: 'wire format check' },
    }

    const wirePublisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      payloadStoreConfig,
      creationConfig: { queue: { QueueName: wireQueueName } },
    })
    await wirePublisher.init()
    await wirePublisher.publish(message)

    // Read the raw SQS message before any consumer touches it
    const { Messages } = await diContainer.cradle.sqsClient.send(
      new ReceiveMessageCommand({
        QueueUrl: wirePublisher.queueProps.url,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      }),
    )
    expect(Messages, 'Expected a message to be in the queue').toBeDefined()
    expect(Messages!.length).toBe(1)

    // SQS body must be a plain JSON pointer — not a codec envelope.
    // Compressed bytes live in S3; only the pointer is sent inline.
    const sqsBody = JSON.parse(Messages![0]!.Body!) as Record<string, unknown>
    expect(
      sqsBody.__mqtCodec,
      'SQS body must not be a codec envelope when offloading',
    ).toBeUndefined()
    expect(sqsBody.payloadRef, 'SQS body must contain a payloadRef pointer').toBeDefined()
    const payloadRef = sqsBody.payloadRef as Record<string, unknown>
    expect(payloadRef.codec).toBe(MessageCodecEnum.ZSTD)

    // S3 object must be raw compressed binary, not a JSON codec envelope.
    // zstd frames start with magic number 0xFD2FB528 (little-endian: 28 B5 2F FD).
    const s3Keys = await waitForS3Objects(s3, s3BucketName, 1, 5000)
    expect(s3Keys.length).toBeGreaterThan(0)
    const s3Bytes = await getObjectBuffer(s3, s3BucketName, s3Keys[0]!)
    expect(s3Bytes.subarray(0, 4)).toEqual(Buffer.from([0x28, 0xb5, 0x2f, 0xfd]))

    await wirePublisher.close()
  }, 30_000)

  it('compresses payload, offloads to S3 as raw binary, and consumer decompresses correctly', async () => {
    const queueName = 'codec-offload-roundtrip'
    await testAdmin.deleteQueues(queueName)

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-offload-1',
      messageType: 'add',
      metadata: { info: 'compressed and offloaded' },
    }

    const publisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      payloadStoreConfig,
      creationConfig: { queue: { QueueName: queueName } },
    })
    // No codec on consumer — codec is read from payloadRef.codec in the pointer
    const consumer = new SqsPermissionConsumer(diContainer.cradle, {
      payloadStoreConfig,
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
    })

    await publisher.init()
    await consumer.start()
    await publisher.publish(message)

    // Verify payload was offloaded to S3
    const s3Keys = await waitForS3Objects(s3, s3BucketName, 1, 5000)
    expect(s3Keys.length).toBeGreaterThan(0)

    // Verify consumer receives the correct decompressed payload
    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await publisher.close()
    await consumer.close(true)
  }, 30_000)

  it('consumer without explicit codec still decompresses codec-offloaded payload', async () => {
    const queueName = 'codec-offload-auto-detect'
    await testAdmin.deleteQueues(queueName)

    const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
      id: 'codec-offload-auto-1',
      messageType: 'add',
      metadata: { info: 'auto-detect codec from pointer' },
    }

    const publisher = new SqsPermissionPublisher(diContainer.cradle, {
      codec: MessageCodecEnum.ZSTD,
      payloadStoreConfig,
      creationConfig: { queue: { QueueName: queueName } },
    })
    // Consumer has no explicit codec — should still work because codec comes from payloadRef.codec
    const consumer = new SqsPermissionConsumer(diContainer.cradle, {
      payloadStoreConfig,
      creationConfig: { queue: { QueueName: queueName } },
      deletionConfig: { deleteIfExists: false },
    })

    await publisher.init()
    await consumer.start()

    await publisher.publish(message)

    const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
    expect(result.message).toMatchObject(message)

    await publisher.close()
    await consumer.close(true)
  }, 30_000)
})
