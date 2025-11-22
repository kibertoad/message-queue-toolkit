import type { PubSub } from '@google-cloud/pubsub'
import type { Storage } from '@google-cloud/storage'
import type { PayloadStoreConfig } from '@message-queue-toolkit/core'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { PUBSUB_MESSAGE_MAX_SIZE } from '../../lib/pubsub/AbstractPubSubService.ts'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import { assertBucket, emptyBucket } from '../utils/gcsUtils.ts'
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
    let pubSubClient: PubSub
    let payloadStoreConfig: PayloadStoreConfig

    let publisher: PubSubPermissionPublisher
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
      }
    })

    beforeEach(async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig,
      })
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig,
      })

      await deletePubSubTopicAndSubscription(
        pubSubClient,
        PubSubPermissionConsumer.TOPIC_NAME,
        PubSubPermissionConsumer.SUBSCRIPTION_NAME,
      )

      await publisher.init()
      await consumer.init()
      await consumer.start()
    })

    afterEach(async () => {
      await publisher.close()
      await consumer.close()
    })

    afterAll(async () => {
      await emptyBucket(gcsStorage, gcsBucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('consumes large message with offloaded payload', async () => {
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

    it('consumes normal-sized message without offloading', async () => {
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

    it('consumes offloaded message with array field and validates schema correctly', async () => {
      // Create a large array of userIds to trigger offloading
      const largeUserIdArray = Array.from({ length: 10000 }, (_, i) => `user-${i}`)

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
    })

    it('validates schema correctly after retrieving offloaded payload', async () => {
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
    })
  })
})
