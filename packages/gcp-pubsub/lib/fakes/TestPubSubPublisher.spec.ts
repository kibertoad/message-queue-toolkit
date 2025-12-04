import type { PubSub } from '@google-cloud/pubsub'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { PubSubPermissionConsumer } from '../../test/consumers/PubSubPermissionConsumer.ts'
import { PubSubPermissionPublisher } from '../../test/publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../../test/utils/cleanupPubSub.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import { TestPubSubPublisher } from './TestPubSubPublisher.ts'

describe('TestPubSubPublisher', () => {
  const topicName = 'test-publisher-topic'
  const subscriptionName = 'test-publisher-sub'

  let diContainer: AwilixContainer<Dependencies>
  let pubSubClient: PubSub
  let testPublisher: TestPubSubPublisher
  let consumer: PubSubPermissionConsumer

  beforeEach(async () => {
    diContainer = await registerDependencies()
    pubSubClient = diContainer.cradle.pubSubClient
    testPublisher = new TestPubSubPublisher(pubSubClient)
    await deletePubSubTopicAndSubscription(pubSubClient, topicName, subscriptionName)

    consumer = new PubSubPermissionConsumer(diContainer.cradle, {
      creationConfig: {
        topic: { name: topicName },
        subscription: { name: subscriptionName },
      },
    })
    await consumer.start()
  })

  afterEach(async () => {
    await consumer.close()
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('publish with topicName', () => {
    it('publishes valid message that consumer can process', async () => {
      const message = {
        id: 'test-valid-1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await testPublisher.publish(message, { topicName })

      const result = await consumer.handlerSpy.waitForMessageWithId('test-valid-1', 'consumed')
      expect(result.message).toMatchObject({ id: 'test-valid-1', messageType: 'add' })
    })

    it('publishes message without validation - invalid messages are acked as errors', async () => {
      // Publish a message missing required fields - consumer will ack it as invalid
      await testPublisher.publish({ incomplete: 'message' }, { topicName })

      // Give consumer time to process and ack the invalid message
      await new Promise((resolve) => setTimeout(resolve, 500))

      // Verify consumer is still working by sending a valid message
      const validMessage = {
        id: 'test-after-invalid',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }
      await testPublisher.publish(validMessage, { topicName })

      const result = await consumer.handlerSpy.waitForMessageWithId(
        'test-after-invalid',
        'consumed',
      )
      expect(result.message.id).toBe('test-after-invalid')
    })
  })

  describe('publish with consumer', () => {
    it('publishes to topic extracted from consumer', async () => {
      const message = {
        id: 'test-consumer-extract',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await testPublisher.publish(message, { consumer })

      const result = await consumer.handlerSpy.waitForMessageWithId(
        'test-consumer-extract',
        'consumed',
      )
      expect(result.message.id).toBe('test-consumer-extract')
    })

    it('throws error when consumer is not initialized', async () => {
      const uninitializedConsumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: 'other-topic' },
          subscription: { name: 'other-sub' },
        },
      })

      await expect(
        testPublisher.publish({ test: 'data' }, { consumer: uninitializedConsumer }),
      ).rejects.toThrow('Consumer has not been initialized')
    })
  })

  describe('publish with publisher', () => {
    it('publishes to topic extracted from publisher', async () => {
      const regularPublisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: topicName },
        },
      })
      await regularPublisher.init()

      const message = {
        id: 'test-publisher-extract',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await testPublisher.publish(message, { publisher: regularPublisher })

      const result = await consumer.handlerSpy.waitForMessageWithId(
        'test-publisher-extract',
        'consumed',
      )
      expect(result.message.id).toBe('test-publisher-extract')
    })

    it('throws error when publisher is not initialized', async () => {
      const uninitializedPublisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: topicName },
        },
      })

      await expect(
        testPublisher.publish({ test: 'data' }, { publisher: uninitializedPublisher }),
      ).rejects.toThrow('Publisher has not been initialized')
    })
  })

  describe('error handling', () => {
    it('throws error when no topic specified', async () => {
      await expect(
        // @ts-expect-error - Testing invalid input
        testPublisher.publish({ test: 'data' }, {}),
      ).rejects.toThrow('Either topicName, consumer, or publisher must be provided')
    })
  })
})
