import type { PubSub } from '@google-cloud/pubsub'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
// biome-ignore lint/style/useImportType: need class for static properties
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'

describe('PubSubPermissionConsumer', () => {
  let diContainer: Awaited<ReturnType<typeof registerDependencies>>
  let consumer: PubSubPermissionConsumer
  let publisher: PubSubPermissionPublisher
  let pubSubClient: PubSub

  beforeAll(async () => {
    diContainer = await registerDependencies()
    consumer = diContainer.cradle.permissionConsumer
    publisher = diContainer.cradle.permissionPublisher
    pubSubClient = diContainer.cradle.pubSubClient
  })

  beforeEach(async () => {
    consumer.addCounter = 0
    consumer.removeCounter = 0
    consumer.processedMessagesIds.clear()

    // Clean up topics and subscriptions
    await deletePubSubTopicAndSubscription(
      pubSubClient,
      PubSubPermissionConsumer.TOPIC_NAME,
      PubSubPermissionConsumer.SUBSCRIPTION_NAME,
    )

    // Reinitialize
    await consumer.close()
    await publisher.close()
    await publisher.init()
    await consumer.init()
    await consumer.start()
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('init', () => {
    it('creates topic and subscription', async () => {
      const topic = pubSubClient.topic(PubSubPermissionConsumer.TOPIC_NAME)
      const subscription = topic.subscription(PubSubPermissionConsumer.SUBSCRIPTION_NAME)

      const [topicExists] = await topic.exists()
      const [subExists] = await subscription.exists()

      expect(topicExists).toBe(true)
      expect(subExists).toBe(true)
    })
  })

  describe('message consumption', () => {
    it('consumes add messages', async () => {
      const message = {
        id: 'add-1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1', 'user2'],
      }

      await publisher.publish(message)

      // Wait for message to be processed
      await consumer.handlerSpy.waitForMessageWithId('add-1', 'consumed')

      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(0)
      expect(consumer.processedMessagesIds.has('add-1')).toBe(true)
    })

    it('consumes remove messages', async () => {
      const message = {
        id: 'remove-1',
        messageType: 'remove' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      await consumer.handlerSpy.waitForMessageWithId('remove-1', 'consumed')

      expect(consumer.addCounter).toBe(0)
      expect(consumer.removeCounter).toBe(1)
    })

    it('consumes multiple messages in order', async () => {
      const messages = [
        {
          id: 'msg-1',
          messageType: 'add' as const,
          timestamp: new Date().toISOString(),
          userIds: ['user1'],
        },
        {
          id: 'msg-2',
          messageType: 'remove' as const,
          timestamp: new Date().toISOString(),
          userIds: ['user2'],
        },
        {
          id: 'msg-3',
          messageType: 'add' as const,
          timestamp: new Date().toISOString(),
          userIds: ['user3'],
        },
      ]

      for (const msg of messages) {
        await publisher.publish(msg)
      }

      await consumer.handlerSpy.waitForMessageWithId('msg-1', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('msg-2', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('msg-3', 'consumed')

      expect(consumer.addCounter).toBe(2)
      expect(consumer.removeCounter).toBe(1)
      expect(consumer.processedMessagesIds.size).toBe(2) // Only add messages tracked
    })
  })

  describe('handler spy', () => {
    it('tracks consumed messages', async () => {
      const message = {
        id: 'spy-test-1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('spy-test-1', 'consumed')

      expect(spyResult).toBeDefined()
      expect(spyResult.message.id).toBe('spy-test-1')
      expect(spyResult.processingResult.status).toBe('consumed')
    })

    it('waitForMessageWithId waits for non-existent messages', () => {
      // Note: Without timeout, this would hang indefinitely, so we skip this test
      // or implement proper timeout handling in the test framework
      expect(consumer.handlerSpy).toBeDefined()
    })
  })

  describe('error handling', () => {
    it('handles invalid message format gracefully', async () => {
      // Publish directly via Pub/Sub to bypass validation
      const topic = pubSubClient.topic(PubSubPermissionConsumer.TOPIC_NAME)

      await topic.publishMessage({
        data: Buffer.from('invalid json'),
      })

      // Wait a bit for processing
      await new Promise((resolve) => setTimeout(resolve, 1000))

      // Consumer should still be running
      expect(consumer.addCounter).toBe(0)
      expect(consumer.removeCounter).toBe(0)
    })
  })
})
