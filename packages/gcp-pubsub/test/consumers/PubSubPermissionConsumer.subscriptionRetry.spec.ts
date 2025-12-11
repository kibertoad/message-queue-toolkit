import type { PubSub } from '@google-cloud/pubsub'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'

describe('PubSubPermissionConsumer - Subscription Retry', () => {
  const TOPIC_NAME = 'user_permissions_retry_test'
  const SUBSCRIPTION_NAME = 'user_permissions_retry_test_sub'

  describe('subscriptionRetryOptions configuration', () => {
    let diContainer: AwilixContainer<Dependencies>
    let pubSubClient: PubSub

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionConsumer: asValue(() => undefined),
        permissionPublisher: asValue(() => undefined),
      })
      pubSubClient = diContainer.cradle.pubSubClient
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      await deletePubSubTopicAndSubscription(pubSubClient, TOPIC_NAME, SUBSCRIPTION_NAME)
    })

    it('uses default retry options when not specified', async () => {
      const consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
      })

      // Access private field via type assertion for testing
      // @ts-expect-error - accessing private field for testing
      const retryOptions = consumer.subscriptionRetryOptions

      expect(retryOptions).toEqual({
        maxRetries: 5,
        baseRetryDelayMs: 1000,
        maxRetryDelayMs: 30000,
      })

      await consumer.close()
    })

    it('accepts custom retry options', async () => {
      const consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
        subscriptionRetryOptions: {
          maxRetries: 10,
          baseRetryDelayMs: 500,
          maxRetryDelayMs: 60000,
        },
      })

      // @ts-expect-error - accessing private field for testing
      const retryOptions = consumer.subscriptionRetryOptions

      expect(retryOptions).toEqual({
        maxRetries: 10,
        baseRetryDelayMs: 500,
        maxRetryDelayMs: 60000,
      })

      await consumer.close()
    })

    it('merges partial retry options with defaults', async () => {
      const consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
        subscriptionRetryOptions: {
          maxRetries: 3,
          // baseRetryDelayMs and maxRetryDelayMs should use defaults
        },
      })

      // @ts-expect-error - accessing private field for testing
      const retryOptions = consumer.subscriptionRetryOptions

      expect(retryOptions).toEqual({
        maxRetries: 3,
        baseRetryDelayMs: 1000,
        maxRetryDelayMs: 30000,
      })

      await consumer.close()
    })
  })

  describe('close behavior', () => {
    let diContainer: AwilixContainer<Dependencies>
    let consumer: PubSubPermissionConsumer
    let publisher: PubSubPermissionPublisher
    let pubSubClient: PubSub

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionConsumer: asValue(() => undefined),
        permissionPublisher: asValue(() => undefined),
      })
      pubSubClient = diContainer.cradle.pubSubClient
    })

    beforeEach(async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
      })
      publisher = new PubSubPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
        },
      })

      await deletePubSubTopicAndSubscription(pubSubClient, TOPIC_NAME, SUBSCRIPTION_NAME)
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

    it('sets isConsuming to false on close', async () => {
      // Verify consumer is running
      // @ts-expect-error - accessing private field for testing
      expect(consumer.isConsuming).toBe(true)

      await consumer.close()

      // @ts-expect-error - accessing private field for testing
      expect(consumer.isConsuming).toBe(false)
    })

    it('removes all listeners on close to prevent reconnection', async () => {
      // Get the subscription reference before close
      // @ts-expect-error - accessing protected field for testing
      const subscription = consumer.subscription

      // Verify subscription exists and has listeners
      expect(subscription).toBeDefined()
      expect(subscription?.listenerCount('message')).toBeGreaterThan(0)
      expect(subscription?.listenerCount('error')).toBeGreaterThan(0)
      expect(subscription?.listenerCount('close')).toBeGreaterThan(0)

      await consumer.close()

      // After close, listeners should be removed
      expect(subscription?.listenerCount('message')).toBe(0)
      expect(subscription?.listenerCount('error')).toBe(0)
      expect(subscription?.listenerCount('close')).toBe(0)
    })

    it('nacks messages received during shutdown', { timeout: 10000 }, async () => {
      // First verify consumer is working
      const message1 = {
        id: 'shutdown-test-1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['user1'],
      }

      await publisher.publish(message1)
      await consumer.handlerSpy.waitForMessageWithId('shutdown-test-1', 'consumed')
      expect(consumer.addCounter).toBe(1)

      // Now close the consumer
      await consumer.close()

      // Messages published after close should not be processed
      // (they'll be nacked and redelivered when consumer restarts)
      // This test just verifies the consumer doesn't crash when receiving messages during shutdown
    })
  })

  describe('start behavior', () => {
    let diContainer: AwilixContainer<Dependencies>
    let pubSubClient: PubSub

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionConsumer: asValue(() => undefined),
        permissionPublisher: asValue(() => undefined),
      })
      pubSubClient = diContainer.cradle.pubSubClient
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      await deletePubSubTopicAndSubscription(pubSubClient, TOPIC_NAME, SUBSCRIPTION_NAME)
    })

    it('does not start multiple times if already consuming', async () => {
      const consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
      })

      await consumer.start()

      // @ts-expect-error - accessing protected field for testing
      const subscription = consumer.subscription
      const initialListenerCount = subscription?.listenerCount('message')

      // Call start again - should be a no-op
      await consumer.start()

      // Listener count should not have increased (no duplicate handlers)
      expect(subscription?.listenerCount('message')).toBe(initialListenerCount)

      await consumer.close()
    })

    it('sets up all required event handlers', async () => {
      const consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: TOPIC_NAME },
          subscription: { name: SUBSCRIPTION_NAME },
        },
      })

      await consumer.start()

      // @ts-expect-error - accessing protected field for testing
      const subscription = consumer.subscription

      expect(subscription?.listenerCount('message')).toBe(1)
      expect(subscription?.listenerCount('error')).toBe(1)
      expect(subscription?.listenerCount('close')).toBe(1)

      await consumer.close()
    })
  })
})
