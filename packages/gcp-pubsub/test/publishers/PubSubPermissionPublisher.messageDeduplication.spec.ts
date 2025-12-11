import { randomUUID } from 'node:crypto'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { type AwilixContainer, asValue } from 'awilix'
import type { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { deletePubSubTopic } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionPublisher } from './PubSubPermissionPublisher.ts'

describe('PubSubPermissionPublisher - Message Deduplication', () => {
  const TOPIC_NAME = 'publisher_dedup_test'

  let diContainer: AwilixContainer<Dependencies>
  let redis: Redis
  let messageDeduplicationStore: RedisMessageDeduplicationStore

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
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
    const keys = await redis.keys('*publisher*')
    if (keys.length > 0) {
      await redis.del(...keys)
    }
  }

  describe('publisher deduplication', () => {
    let publisher: PubSubPermissionPublisher

    beforeEach(async () => {
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
        },
        messageDeduplicationConfig: {
          deduplicationStore: messageDeduplicationStore,
        },
        enablePublisherDeduplication: true,
      })

      await deletePubSubTopic(diContainer.cradle.pubSubClient, TOPIC_NAME)
      await cleanRedis()
      await publisher.init()
    })

    afterEach(async () => {
      vi.restoreAllMocks()
      await publisher.close()
      await cleanRedis()
    })

    it('publishes message and stores deduplication key', async () => {
      const messageId = randomUUID()
      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      const spy = await publisher.handlerSpy.waitForMessageWithId(messageId, 'published')
      expect(spy.processingResult).toEqual({ status: 'published' })

      // Verify deduplication key was stored
      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `publisher:${messageId}`,
      )
      expect(deduplicationKeyExists).toBe(true)
    })

    it('skips duplicate message when already published', async () => {
      const messageId = randomUUID()
      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      // First publish
      await publisher.publish(message)

      const firstSpy = await publisher.handlerSpy.waitForMessageWithId(messageId, 'published')
      expect(firstSpy.processingResult).toEqual({ status: 'published' })

      // Clear spy for subsequent call
      publisher.handlerSpy.clear()

      // Second publish - should be skipped as duplicate
      await publisher.publish(message)

      const secondSpy = await publisher.handlerSpy.waitForMessageWithId(messageId, 'published')
      expect(secondSpy.processingResult).toEqual({
        status: 'published',
        skippedAsDuplicate: true,
      })
    })

    it('publishes messages with different IDs independently', async () => {
      const message1: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: randomUUID(),
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }
      const message2: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: randomUUID(),
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user2'],
      }

      // First message
      await publisher.publish(message1)
      const spy1 = await publisher.handlerSpy.waitForMessageWithId(message1.id, 'published')
      expect(spy1.processingResult).toEqual({ status: 'published' })

      publisher.handlerSpy.clear()

      // Second message with different ID
      await publisher.publish(message2)
      const spy2 = await publisher.handlerSpy.waitForMessageWithId(message2.id, 'published')
      expect(spy2.processingResult).toEqual({ status: 'published' })
    })
  })

  describe('publisher without deduplication', () => {
    let publisher: PubSubPermissionPublisher

    beforeEach(async () => {
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
        },
        // No deduplication config
      })

      await deletePubSubTopic(diContainer.cradle.pubSubClient, TOPIC_NAME)
      await cleanRedis()
      await publisher.init()
    })

    afterEach(async () => {
      await publisher.close()
      await cleanRedis()
    })

    it('publishes duplicate messages when deduplication is disabled', async () => {
      const messageId = randomUUID()
      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: messageId,
        messageType: 'add',
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      // First publish
      await publisher.publish(message)
      const firstSpy = await publisher.handlerSpy.waitForMessageWithId(messageId, 'published')
      expect(firstSpy.processingResult).toEqual({ status: 'published' })

      publisher.handlerSpy.clear()

      // Second publish - should also be published (no deduplication)
      await publisher.publish(message)
      const secondSpy = await publisher.handlerSpy.waitForMessageWithId(messageId, 'published')
      expect(secondSpy.processingResult).toEqual({ status: 'published' })

      // Verify no deduplication key was stored
      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `publisher:${messageId}`,
      )
      expect(deduplicationKeyExists).toBe(false)
    })
  })
})
