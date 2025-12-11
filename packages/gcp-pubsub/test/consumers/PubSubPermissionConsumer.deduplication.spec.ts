import { randomUUID } from 'node:crypto'
import type { PubSub } from '@google-cloud/pubsub'
import {
  AcquireLockTimeoutError,
  type MessageDeduplicationConfig,
} from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import type { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('PubSubPermissionConsumer - Deduplication', () => {
  const TOPIC_NAME = 'user_permissions_dedup_test'
  const SUBSCRIPTION_NAME = 'user_permissions_dedup_test_sub'

  let diContainer: AwilixContainer<Dependencies>
  let pubSubClient: PubSub
  let redis: Redis
  let messageDeduplicationStore: RedisMessageDeduplicationStore

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionConsumer: asValue(() => undefined),
      permissionPublisher: asValue(() => undefined),
    })
    pubSubClient = diContainer.cradle.pubSubClient
    redis = diContainer.cradle.redis
    messageDeduplicationStore = new RedisMessageDeduplicationStore({
      redis,
    })
  })

  afterAll(async () => {
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  async function cleanRedis() {
    const keys = await redis.keys('*consumer*')
    if (keys.length > 0) {
      await redis.del(...keys)
    }
    const mutexKeys = await redis.keys('*mutex*')
    if (mutexKeys.length > 0) {
      await redis.del(...mutexKeys)
    }
  }

  describe('consumer deduplication', () => {
    let consumer: PubSubPermissionConsumer
    let publisher: PubSubPermissionPublisher
    let messageDeduplicationConfig: MessageDeduplicationConfig

    beforeEach(async () => {
      messageDeduplicationConfig = {
        deduplicationStore: messageDeduplicationStore,
      }

      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
        enableConsumerDeduplication: true,
        messageDeduplicationConfig,
        // Use 'id' field as deduplication ID since our schema doesn't have deduplicationId
        messageDeduplicationIdField: 'id',
      })
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
        },
      })

      await deletePubSubTopicAndSubscription(pubSubClient, TOPIC_NAME, SUBSCRIPTION_NAME)
      await cleanRedis()
      await consumer.start()
      await publisher.init()
    })

    afterEach(async () => {
      await consumer.close()
      await publisher.close()
      await cleanRedis()
    })

    it('processes message normally when deduplication is enabled', async () => {
      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: randomUUID(),
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      const result = await consumer.handlerSpy.waitForMessageWithId(message.id, 'consumed')
      expect(result.processingResult.status).toBe('consumed')
      expect(consumer.addCounter).toBe(1)
    })

    it('skips duplicate message when already processed', async () => {
      const messageId = randomUUID()
      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      // First publish
      await publisher.publish(message)

      // Message is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(messageId)
      expect(firstConsumptionResult.processingResult).toEqual({ status: 'consumed' })
      expect(consumer.addCounter).toBe(1)

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      // Publish again - should be skipped as duplicate
      await publisher.publish(message)

      // Message is not processed due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(messageId)
      expect(secondConsumptionResult.processingResult).toEqual({
        status: 'consumed',
        skippedAsDuplicate: true,
      })

      // Handler should only have been called once
      expect(consumer.addCounter).toBe(1)
    })
  })

  describe('lock acquisition', () => {
    let consumer: PubSubPermissionConsumer
    let publisher: PubSubPermissionPublisher
    let messageDeduplicationConfig: MessageDeduplicationConfig

    beforeEach(async () => {
      messageDeduplicationConfig = {
        deduplicationStore: messageDeduplicationStore,
      }

      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
        enableConsumerDeduplication: true,
        messageDeduplicationConfig,
        // Use 'id' field as deduplication ID since our schema doesn't have deduplicationId
        messageDeduplicationIdField: 'id',
      })
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
        },
      })

      await deletePubSubTopicAndSubscription(pubSubClient, TOPIC_NAME, SUBSCRIPTION_NAME)
      await cleanRedis()
      await consumer.start()
      await publisher.init()
    })

    afterEach(async () => {
      vi.restoreAllMocks()
      await consumer.close()
      await publisher.close()
      await cleanRedis()
    })

    it('acquires and releases lock during message processing', { timeout: 15000 }, async () => {
      const messageId = randomUUID()

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      const result = await consumer.handlerSpy.waitForMessageWithId(messageId, 'consumed')
      expect(result.processingResult.status).toBe('consumed')
      expect(consumer.addCounter).toBe(1)

      // After successful processing, the lock should be released
      // but the deduplication key should still exist
      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `consumer:${messageId}`,
      )
      expect(deduplicationKeyExists).toBe(true)
    })

    it('nacks message when lock acquisition times out', { timeout: 15000 }, async () => {
      const messageId = randomUUID()

      // Mock acquireLock to simulate timeout (another consumer holds the lock)
      // Only AcquireLockTimeoutError causes retry - regular errors are swallowed and message is processed
      vi.spyOn(messageDeduplicationStore, 'acquireLock').mockResolvedValue({
        error: new AcquireLockTimeoutError('Lock acquisition timeout'),
      })

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      // Wait a bit for processing attempts - message should be nacked and redelivered
      await new Promise((resolve) => setTimeout(resolve, 2000))

      // Handler should not have been called because lock couldn't be acquired
      expect(consumer.addCounter).toBe(0)

      // Restore the mock so the message can be processed
      vi.restoreAllMocks()

      // Now the message should be processed on redelivery
      const result = await consumer.handlerSpy.waitForMessageWithId(messageId, 'consumed')
      expect(result.processingResult.status).toBe('consumed')
      expect(consumer.addCounter).toBe(1)
    })

    it(
      'processes message when lock acquisition has non-timeout error',
      { timeout: 15000 },
      async () => {
        const messageId = randomUUID()

        // Mock acquireLock to simulate a non-timeout error (e.g., Redis connection error)
        // Non-timeout errors are swallowed and message is processed normally
        vi.spyOn(messageDeduplicationStore, 'acquireLock').mockResolvedValue({
          error: new Error('Redis connection error'),
        })

        const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
          id: messageId,
          messageType: 'add',
          timestamp: new Date().toISOString(),
          userIds: ['user1'],
        }

        await publisher.publish(message)

        // Message should be processed even though lock acquisition failed
        const result = await consumer.handlerSpy.waitForMessageWithId(messageId, 'consumed')
        expect(result.processingResult.status).toBe('consumed')
        expect(consumer.addCounter).toBe(1)
      },
    )
  })
})
