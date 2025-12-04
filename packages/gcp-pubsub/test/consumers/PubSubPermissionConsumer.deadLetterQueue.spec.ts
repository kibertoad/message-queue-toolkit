import type { PubSub } from '@google-cloud/pubsub'
import type { Either } from '@lokalise/node-core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { z } from 'zod/v4'
import type { PubSubConsumerDependencies } from '../../lib/pubsub/AbstractPubSubConsumer.ts'
import { AbstractPubSubConsumer } from '../../lib/pubsub/AbstractPubSubConsumer.ts'
import { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

// Simple schema that accepts any JSON object for DLQ messages
const DLQ_MESSAGE_SCHEMA = z
  .object({
    id: z.string(),
    messageType: z.string(),
    timestamp: z.string().optional(),
  })
  .passthrough()

type DlqMessage = z.infer<typeof DLQ_MESSAGE_SCHEMA>

// Execution context type for DLQ consumer (empty, not needed)
type DlqExecutionContext = Record<string, never>

// Simple DLQ consumer for testing
class DlqConsumer extends AbstractPubSubConsumer<DlqMessage, DlqExecutionContext> {
  public receivedMessages: DlqMessage[] = []

  constructor(
    dependencies: PubSubConsumerDependencies,
    topicName: string,
    subscriptionName: string,
  ) {
    super(
      dependencies,
      {
        creationConfig: {
          topic: { name: topicName },
          subscription: { name: subscriptionName },
        },
        messageTypeField: 'messageType',
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<DlqMessage, DlqExecutionContext>()
          .addConfig(DLQ_MESSAGE_SCHEMA, (message): Promise<Either<'retryLater', 'success'>> => {
            this.receivedMessages.push(message)
            return Promise.resolve({ result: 'success' })
          })
          .build(),
      },
      {} as DlqExecutionContext,
    )
  }
}

describe('PubSubPermissionConsumer - Dead Letter Queue', () => {
  const queueName = PubSubPermissionConsumer.TOPIC_NAME
  const subscriptionName = PubSubPermissionConsumer.SUBSCRIPTION_NAME
  const deadLetterTopicName = `${queueName}-dlq`
  const deadLetterSubscriptionName = `${subscriptionName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let pubSubClient: PubSub
  let publisher: PubSubPermissionPublisher
  let consumer: PubSubPermissionConsumer | undefined
  let dlqConsumer: DlqConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionConsumer: asValue(() => undefined),
      permissionPublisher: asValue(() => undefined),
    })
    pubSubClient = diContainer.cradle.pubSubClient
  })

  beforeEach(async () => {
    // Create publisher instance first
    publisher = new PubSubPermissionPublisher(diContainer.cradle)

    // Delete resources after creating instances but before init
    await deletePubSubTopicAndSubscription(pubSubClient, queueName, subscriptionName)
    await deletePubSubTopicAndSubscription(
      pubSubClient,
      deadLetterTopicName,
      deadLetterSubscriptionName,
    )

    // Init publisher (consumer is created per-test with different options)
    await publisher.init()
  })

  afterEach(async () => {
    await consumer?.close()
    await dlqConsumer?.close()
    await publisher?.close()
    consumer = undefined
    dlqConsumer = undefined
  })

  afterAll(async () => {
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('init', () => {
    it('creates dead letter topic', async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
      })

      await consumer.init()

      expect(consumer.dlqTopicName).toBe(deadLetterTopicName)

      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      const [topicExists] = await dlqTopic.exists()
      expect(topicExists).toBe(true)

      // Verify subscription has deadLetterPolicy configured
      const subscription = pubSubClient.subscription(subscriptionName)
      const [metadata] = await subscription.getMetadata()
      expect(metadata.deadLetterPolicy).toBeDefined()
      expect(metadata.deadLetterPolicy?.maxDeliveryAttempts).toBe(5)
      expect(metadata.deadLetterPolicy?.deadLetterTopic).toContain(deadLetterTopicName)
    })

    it('throws an error when invalid dlq locator is passed', async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          locatorConfig: { topicName: 'nonexistent-topic' },
        },
      })

      await expect(() => consumer?.init()).rejects.toThrow(/does not exist/)
    })

    it('uses existing dead letter topic when locator is passed', async () => {
      // Create DLQ topic first
      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      await dlqTopic.create()

      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          locatorConfig: { topicName: deadLetterTopicName },
        },
      })

      await consumer.init()

      expect(consumer.dlqTopicName).toBe(deadLetterTopicName)
    })
  })

  describe('messages with errors on process should go to DLQ', () => {
    it('after errors, messages should go to DLQ', async () => {
      let counter = 0
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
        removeHandlerOverride: () => {
          counter++
          throw new Error('Error')
        },
      })

      // Init consumer first to create DLQ topic, but don't start yet
      await consumer.init()

      // Create and start DLQ consumer BEFORE starting main consumer to avoid race condition
      dlqConsumer = new DlqConsumer(
        diContainer.cradle,
        deadLetterTopicName,
        deadLetterSubscriptionName,
      )
      await dlqConsumer.start()

      // Now start main consumer
      await consumer.start()

      await publisher.publish({
        id: '1',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
        userIds: [],
      })

      // Wait for message to appear in DLQ
      const dlqResult = await dlqConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      expect(dlqResult.message).toMatchObject({
        id: '1',
        messageType: 'remove',
      })
      expect(counter).toBe(5)
    })

    it('messages with retryLater should be retried and not go to DLQ', async () => {
      const pubsubMessage: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
        userIds: [],
      }

      let counter = 0
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 10 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
        removeHandlerOverride: (message) => {
          if (message.id !== pubsubMessage.id) {
            throw new Error('not expected message')
          }
          counter++
          return counter < 2
            ? Promise.resolve({ error: 'retryLater' })
            : Promise.resolve({ result: 'success' })
        },
      })
      await consumer.start()

      await publisher.publish(pubsubMessage)

      const handlerSpyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(handlerSpyResult.processingResult).toEqual({ status: 'consumed' })
      expect(handlerSpyResult.message).toMatchObject({ id: '1', messageType: 'remove' })

      expect(counter).toBe(2)
      // Note: Pub/Sub emulator doesn't implement exponential backoff, so we don't test timing here
    })

    it('messages with deserialization errors should go to DLQ', async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
      })

      // Init consumer first to create DLQ topic, but don't start yet
      await consumer.init()

      // Create and start DLQ consumer BEFORE starting main consumer
      dlqConsumer = new DlqConsumer(
        diContainer.cradle,
        deadLetterTopicName,
        deadLetterSubscriptionName,
      )
      await dlqConsumer.start()

      // Now start main consumer
      await consumer.start()

      // Publish invalid message directly
      const topic = pubSubClient.topic(queueName)
      await topic.publishMessage({
        data: Buffer.from(JSON.stringify({ id: '1', messageType: 'bad' })),
      })

      // Wait for message to appear in DLQ
      const dlqResult = await dlqConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      expect(dlqResult.message).toMatchObject({
        id: '1',
        messageType: 'bad',
      })
    })
  })

  describe('messages stuck should be marked as consumed and go to DLQ', () => {
    it('messages stuck on barrier', async () => {
      let counter = 0
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
        maxRetryDuration: 1, // 1 second - message will expire quickly
        addPreHandlerBarrier: (_msg) => {
          counter++
          return Promise.resolve({ isPassing: false })
        },
      })

      // Init consumer first to create DLQ topic, but don't start yet
      await consumer.init()

      // Create and start DLQ consumer BEFORE starting main consumer
      dlqConsumer = new DlqConsumer(
        diContainer.cradle,
        deadLetterTopicName,
        deadLetterSubscriptionName,
      )
      await dlqConsumer.start()

      // Now start main consumer
      await consumer.start()

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: '1',
        messageType: 'add',
        // Message timestamp 2 seconds ago - already past maxRetryDuration
        timestamp: new Date(Date.now() - 2000).toISOString(),
      }
      await publisher.publish(message)

      // Verify error is tracked by main consumer
      const spyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spyResult.message).toEqual(message)
      expect(counter).toBeGreaterThanOrEqual(1)

      // Wait for message to appear in DLQ
      const dlqResult = await dlqConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(dlqResult.message).toMatchObject({
        id: '1',
        messageType: 'add',
        timestamp: message.timestamp,
      })
    }, 15000)

    it('messages stuck on handler', async () => {
      let counter = 0
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 5 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
        maxRetryDuration: 1, // 1 second - message will expire quickly
        removeHandlerOverride: () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
      })

      // Init consumer first to create DLQ topic, but don't start yet
      await consumer.init()

      // Create and start DLQ consumer BEFORE starting main consumer
      dlqConsumer = new DlqConsumer(
        diContainer.cradle,
        deadLetterTopicName,
        deadLetterSubscriptionName,
      )
      await dlqConsumer.start()

      // Now start main consumer
      await consumer.start()

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '2',
        messageType: 'remove',
        // Message timestamp 2 seconds ago - already past maxRetryDuration
        timestamp: new Date(Date.now() - 2000).toISOString(),
        userIds: [],
      }
      await publisher.publish(message)

      // Verify error is tracked by main consumer
      const spyResult = await consumer.handlerSpy.waitForMessageWithId('2', 'error')
      expect(spyResult.message).toEqual(message)
      expect(counter).toBeGreaterThanOrEqual(1)

      // Wait for message to appear in DLQ
      const dlqResult = await dlqConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(dlqResult.message).toMatchObject({
        id: '2',
        messageType: 'remove',
        timestamp: message.timestamp,
      })
    }, 15000)
  })
})
