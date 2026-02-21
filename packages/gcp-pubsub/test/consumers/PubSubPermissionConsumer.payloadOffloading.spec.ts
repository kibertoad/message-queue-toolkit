import type { PubSub } from '@google-cloud/pubsub'
import type { Storage } from '@google-cloud/storage'
import type { PayloadStoreConfig } from '@message-queue-toolkit/core'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { PUBSUB_MESSAGE_MAX_SIZE } from '../../lib/pubsub/AbstractPubSubService.ts'
import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '../../lib/utils/messageUtils.ts'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import { assertBucket, emptyBuckets } from '../utils/gcsUtils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('PubSubPermissionConsumer - Payload Offloading', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = PUBSUB_MESSAGE_MAX_SIZE
    const gcsBucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let gcsStorage: Storage
    let payloadStoreConfig: PayloadStoreConfig

    let publisher: PubSubPermissionPublisher
    let consumer: PubSubPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      gcsStorage = diContainer.cradle.gcsStorage

      await assertBucket(gcsStorage, gcsBucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new GCSPayloadStore(diContainer.cradle, { bucketName: gcsBucketName }),
        storeName: 'gcs',
      }
    })

    beforeEach(async () => {
      // Create instances first
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: PubSubPermissionConsumer.TOPIC_NAME },
          subscription: { name: PubSubPermissionConsumer.SUBSCRIPTION_NAME },
        },
        payloadStoreConfig,
      })
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: PubSubPermissionPublisher.TOPIC_NAME },
        },
        payloadStoreConfig,
      })

      // Delete resources after creating instances but before start/init
      await deletePubSubTopicAndSubscription(
        diContainer.cradle.pubSubClient,
        PubSubPermissionConsumer.TOPIC_NAME,
        PubSubPermissionConsumer.SUBSCRIPTION_NAME,
      )

      await consumer.start()
      await publisher.init()
    })

    afterEach(async () => {
      await consumer.close()
      await publisher.close()
    })

    afterAll(async () => {
      await emptyBuckets(gcsStorage, gcsBucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('consumes large message with offloaded payload', { timeout: 10000 }, async () => {
      // Craft a message that is larger than the max message size
      const message = {
        id: 'large-message-1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
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
      expect(consumer.addCounter).toBe(1)
    })

    it('consumes normal-sized message without offloading', { timeout: 10000 }, async () => {
      const message = {
        id: 'normal-message-1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1', 'user2'],
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      expect(JSON.stringify(message).length).toBeLessThan(largeMessageSizeThreshold)

      await publisher.publish(message)

      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
      expect(consumer.addCounter).toBe(1)
    })

    it(
      'consumes offloaded message with array field and validates schema correctly',
      { timeout: 10000 },
      async () => {
        // Create a large array of userIds to trigger offloading (need > 10MB)
        // Each userId needs to be ~1000 chars to make 10,500 items exceed 10MB
        const largeUserIdArray = Array.from(
          { length: 10500 },
          (_, i) => `user-${i}-${'x'.repeat(1000)}`,
        )

        const message = {
          id: 'large-array-message-1',
          messageType: 'add',
          timestamp: new Date().toISOString(),
          userIds: largeUserIdArray,
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

        // Verify the message is large enough to trigger offloading
        expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

        await publisher.publish(message)

        // Wait for the message to be consumed
        const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
          message.id,
          'consumed',
        )

        // Verify the full payload was received including the large array
        expect(consumptionResult.message).toMatchObject({
          id: message.id,
          messageType: message.messageType,
          userIds: largeUserIdArray,
        })
        expect(consumptionResult.message.userIds).toHaveLength(largeUserIdArray.length)
        expect(consumer.addCounter).toBe(1)
      },
    )

    it(
      'validates schema correctly after retrieving offloaded payload',
      { timeout: 10000 },
      async () => {
        // Create a message with metadata that will be validated against the schema
        const message = {
          id: 'schema-validation-1',
          messageType: 'add',
          timestamp: new Date().toISOString(),
          metadata: {
            largeField: 'x'.repeat(largeMessageSizeThreshold + 1000),
          },
          userIds: ['test-user'],
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

        expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

        await publisher.publish(message)

        const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
          message.id,
          'consumed',
        )

        // Verify all fields were properly deserialized and validated
        expect(consumptionResult.message).toMatchObject({
          id: message.id,
          messageType: message.messageType,
          userIds: message.userIds,
          metadata: {
            largeField: message.metadata.largeField,
          },
        })

        // Type guard to access metadata property
        if (consumptionResult.message.messageType === 'add') {
          expect(consumptionResult.message.metadata?.largeField).toHaveLength(
            message.metadata.largeField.length,
          )
        }
        expect(consumer.addCounter).toBe(1)
      },
    )
  })

  describe('payload retrieval errors', () => {
    const largeMessageSizeThreshold = PUBSUB_MESSAGE_MAX_SIZE
    const gcsBucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let pubSubClient: PubSub
    let gcsStorage: Storage
    let payloadStoreConfig: PayloadStoreConfig
    let consumer: PubSubPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      gcsStorage = diContainer.cradle.gcsStorage
      pubSubClient = diContainer.cradle.pubSubClient

      await assertBucket(gcsStorage, gcsBucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new GCSPayloadStore(diContainer.cradle, { bucketName: gcsBucketName }),
        storeName: 'gcs',
      }
    })

    beforeEach(async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: PubSubPermissionConsumer.TOPIC_NAME },
          subscription: { name: PubSubPermissionConsumer.SUBSCRIPTION_NAME },
        },
        payloadStoreConfig,
      })

      await deletePubSubTopicAndSubscription(
        pubSubClient,
        PubSubPermissionConsumer.TOPIC_NAME,
        PubSubPermissionConsumer.SUBSCRIPTION_NAME,
      )

      await consumer.start()
    })

    afterEach(async () => {
      await consumer.close()
    })

    afterAll(async () => {
      await emptyBuckets(gcsStorage, gcsBucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('handles error when offloaded payload cannot be retrieved', { timeout: 10000 }, async () => {
      // Create a message that looks like it has offloaded payload but the payload doesn't exist
      const topic = pubSubClient.topic(PubSubPermissionConsumer.TOPIC_NAME)

      const messageWithFakeOffload = {
        id: 'fake-offload-1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        // Reference to a non-existent GCS object
        _payloadKey: 'non-existent-key-12345',
      }

      // Publish with the offload attribute to trigger retrieval
      await topic.publishMessage({
        data: Buffer.from(JSON.stringify(messageWithFakeOffload)),
        attributes: {
          [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]: '12345',
        },
      })

      // Wait for the error to be tracked
      const spyResult = await consumer.handlerSpy.waitForMessage({ id: 'fake-offload-1' }, 'error')

      expect(spyResult.processingResult.status).toBe('error')
      // @ts-expect-error field exists
      expect(spyResult.processingResult.errorReason).toBe('invalidMessage')
      expect(consumer.addCounter).toBe(0)
    })
  })
})
