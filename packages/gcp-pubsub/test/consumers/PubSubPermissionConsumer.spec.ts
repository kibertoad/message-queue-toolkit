import type { PubSub } from '@google-cloud/pubsub'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'

describe('PubSubPermissionConsumer', () => {
  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let consumer: PubSubPermissionConsumer
    let publisher: PubSubPermissionPublisher
    let pubSubClient: PubSub

    beforeAll(async () => {
      // Disable auto-init of consumer/publisher from DI - we'll create them manually
      diContainer = await registerDependencies({
        permissionConsumer: asValue(() => undefined),
        permissionPublisher: asValue(() => undefined),
      })
      pubSubClient = diContainer.cradle.pubSubClient
    })

    beforeEach(async () => {
      // Create instances first
      consumer = new PubSubPermissionConsumer(diContainer.cradle)
      publisher = new PubSubPermissionPublisher(diContainer.cradle)

      // Delete resources after creating instances but before start/init
      await deletePubSubTopicAndSubscription(
        pubSubClient,
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
      await diContainer.cradle.awilixManager.executeDispose()
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
      it('consumes add messages', { timeout: 10000 }, async () => {
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

      it('consumes remove messages', { timeout: 10000 }, async () => {
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

      it('consumes multiple messages in order', { timeout: 10000 }, async () => {
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

      it('waitForMessageWithId waits for messages published after the spy starts waiting', async () => {
        const message = {
          id: 'wait-test-1',
          messageType: 'add' as const,
          timestamp: new Date().toISOString(),
          userIds: ['user1'],
        }

        // Start waiting BEFORE publishing
        const spyPromise = consumer.handlerSpy.waitForMessageWithId('wait-test-1', 'consumed')

        // Now publish the message
        await publisher.publish(message)

        // The spy should resolve once the message is processed
        const spyResult = await spyPromise

        expect(spyResult).toBeDefined()
        expect(spyResult.message.id).toBe('wait-test-1')
        expect(spyResult.processingResult.status).toBe('consumed')
      })
    })

    describe('error handling', () => {
      it('handles invalid message format gracefully and acks without DLQ', async () => {
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
        // Message should NOT be redelivered infinitely (ack when no DLQ configured)
      })

      it('tracks schema validation errors with handlerSpy', async () => {
        const topic = pubSubClient.topic(PubSubPermissionConsumer.TOPIC_NAME)

        // Create a message with valid JSON but invalid schema (userIds should be array, not string)
        const invalidMessage = {
          id: 'error-test-1',
          messageType: 'add',
          timestamp: new Date().toISOString(),
          userIds: 'invalid-should-be-array', // Invalid type - should fail validation
        }

        // Start waiting for the error
        const spyPromise = consumer.handlerSpy.waitForMessage({ id: 'error-test-1' }, 'error')

        // Publish the invalid message
        await topic.publishMessage({
          data: Buffer.from(JSON.stringify(invalidMessage)),
        })

        // Wait for the error to be tracked
        const spyResult = await spyPromise

        expect(spyResult).toBeDefined()
        expect(spyResult.processingResult.status).toBe('error')
        // @ts-expect-error field is there
        expect(spyResult.processingResult.errorReason).toBe('invalidMessage')

        // Consumer should still be running and not have processed the message
        expect(consumer.addCounter).toBe(0)
        expect(consumer.removeCounter).toBe(0)
      })

      it('acknowledges invalid messages when no DLQ is configured to prevent infinite redelivery', async () => {
        // This test verifies that without DLQ, invalid messages are acknowledged
        // instead of nacked infinitely
        const topic = pubSubClient.topic(PubSubPermissionConsumer.TOPIC_NAME)

        const invalidMessage = {
          id: 'ack-no-dlq-test',
          messageType: 'unknown-type', // Unknown message type - will fail schema resolution
          timestamp: new Date().toISOString(),
        }

        // Start waiting for the error
        const spyPromise = consumer.handlerSpy.waitForMessage({ id: 'ack-no-dlq-test' }, 'error')

        await topic.publishMessage({
          data: Buffer.from(JSON.stringify(invalidMessage)),
        })

        const spyResult = await spyPromise

        // Verify the error was tracked
        expect(spyResult.processingResult.status).toBe('error')
        // @ts-expect-error field is there
        expect(spyResult.processingResult.errorReason).toBe('invalidMessage')

        // Wait to ensure no redelivery happens (message was acked, not nacked)
        // In the previous behavior, this would cause infinite redelivery without DLQ
        await new Promise((resolve) => setTimeout(resolve, 500))

        // Check the spy only recorded the message once (no redelivery)
        const allMessages = consumer.handlerSpy.getAllReceivedMessages()
        const matchingMessages = allMessages.filter(
          (msg) => (msg.message as { id: string }).id === 'ack-no-dlq-test',
        )
        expect(matchingMessages.length).toBe(1)
      })
    })
  })
})
