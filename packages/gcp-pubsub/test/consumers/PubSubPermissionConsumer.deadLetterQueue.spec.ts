import type { PubSub, Message as PubSubMessageType } from '@google-cloud/pubsub'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { PubSubPermissionPublisher } from '../publishers/PubSubPermissionPublisher.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionConsumer } from './PubSubPermissionConsumer.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

describe('PubSubPermissionConsumer - Dead Letter Queue', () => {
  const queueName = PubSubPermissionConsumer.TOPIC_NAME
  const subscriptionName = PubSubPermissionConsumer.SUBSCRIPTION_NAME
  const deadLetterTopicName = `${queueName}-dlq`
  const deadLetterSubscriptionName = `${subscriptionName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let pubSubClient: PubSub
  let permissionPublisher: PubSubPermissionPublisher
  let consumer: PubSubPermissionConsumer | undefined
  let dlqSubscription: ReturnType<typeof pubSubClient.subscription> | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies()
    pubSubClient = diContainer.cradle.pubSubClient
    permissionPublisher = diContainer.cradle.permissionPublisher
  })

  beforeEach(async () => {
    await deletePubSubTopicAndSubscription(pubSubClient, queueName, subscriptionName)
    await deletePubSubTopicAndSubscription(
      pubSubClient,
      deadLetterTopicName,
      deadLetterSubscriptionName,
    )
  })

  afterEach(async () => {
    await consumer?.close()
    if (dlqSubscription) {
      dlqSubscription.removeAllListeners()
      await dlqSubscription.close()
    }
    dlqSubscription = undefined
    consumer = undefined
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
          deadLetterPolicy: { maxDeliveryAttempts: 2 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
        removeHandlerOverride: () => {
          counter++
          throw new Error('Error')
        },
      })
      await consumer.start()

      // Create DLQ subscription to listen for messages
      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      const [dlqSub] = await dlqTopic.createSubscription(deadLetterSubscriptionName)
      dlqSubscription = dlqSub

      let dlqMessage: PubSubMessageType | undefined
      dlqSubscription.on('message', (message: PubSubMessageType) => {
        dlqMessage = message
        message.ack()
      })

      await permissionPublisher.publish({
        id: '1',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
        userIds: [],
      })

      await waitAndRetry(() => dlqMessage, 50, 40)

      expect(dlqMessage).toBeDefined()
      expect(counter).toBe(2)

      const dlqMessageBody = JSON.parse(dlqMessage!.data.toString())
      expect(dlqMessageBody).toMatchObject({
        id: '1',
        messageType: 'remove',
        timestamp: expect.any(String),
      })
    })

    it('messages with retryLater should be retried with exponential delay and not go to DLQ', async () => {
      const pubsubMessage: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
        userIds: [],
      }

      let counter = 0
      const messageArrivalTime: number[] = []
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
          messageArrivalTime.push(Date.now())
          return counter < 2
            ? Promise.resolve({ error: 'retryLater' })
            : Promise.resolve({ result: 'success' })
        },
      })
      await consumer.start()

      await permissionPublisher.publish(pubsubMessage)

      const handlerSpyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(handlerSpyResult.processingResult).toEqual({ status: 'consumed' })
      expect(handlerSpyResult.message).toMatchObject({ id: '1', messageType: 'remove' })

      expect(counter).toBe(2)

      // Verify retry delay (should be at least 1 second due to exponential backoff)
      const secondsRetry = (messageArrivalTime[1]! - messageArrivalTime[0]!) / 1000
      expect(secondsRetry).toBeGreaterThan(1)
    })

    it('messages with deserialization errors should go to DLQ', async () => {
      consumer = new PubSubPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { name: queueName },
          subscription: { name: subscriptionName },
        },
        deadLetterQueue: {
          deadLetterPolicy: { maxDeliveryAttempts: 2 },
          creationConfig: { topic: { name: deadLetterTopicName } },
        },
      })
      await consumer.start()

      // Create DLQ subscription
      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      const [dlqSub] = await dlqTopic.createSubscription(deadLetterSubscriptionName)
      dlqSubscription = dlqSub

      let dlqMessage: PubSubMessageType | undefined
      dlqSubscription.on('message', (message: PubSubMessageType) => {
        dlqMessage = message
        message.ack()
      })

      // Publish invalid message directly
      const topic = pubSubClient.topic(queueName)
      await topic.publishMessage({
        data: Buffer.from(JSON.stringify({ id: '1', messageType: 'bad' })),
      })

      await waitAndRetry(async () => dlqMessage, 50, 40)

      expect(dlqMessage).toBeDefined()
      expect(dlqMessage!.data.toString()).toBe(JSON.stringify({ id: '1', messageType: 'bad' }))
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
        maxRetryDuration: 2,
        addPreHandlerBarrier: (_msg) => {
          counter++
          return Promise.resolve({ isPassing: false })
        },
      })
      await consumer.start()

      // Create DLQ subscription
      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      const [dlqSub] = await dlqTopic.createSubscription(deadLetterSubscriptionName)
      dlqSubscription = dlqSub

      let dlqMessage: PubSubMessageType | undefined
      dlqSubscription.on('message', (message: PubSubMessageType) => {
        dlqMessage = message
        message.ack()
      })

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: '1',
        messageType: 'add',
        timestamp: new Date(Date.now() - 1000).toISOString(),
      }
      await permissionPublisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spyResult.message).toEqual(message)
      // Due to exponential backoff and timestamp, message is retried a few times before being moved to DLQ
      expect(counter).toBeGreaterThanOrEqual(2)

      await waitAndRetry(() => dlqMessage, 50, 40)
      const messageBody = JSON.parse(dlqMessage!.data.toString())
      expect(messageBody).toMatchObject({
        id: '1',
        messageType: 'add',
        timestamp: message.timestamp,
      })
    })

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
        maxRetryDuration: 2,
        removeHandlerOverride: () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
      })
      await consumer.start()

      // Create DLQ subscription
      const dlqTopic = pubSubClient.topic(deadLetterTopicName)
      const [dlqSub] = await dlqTopic.createSubscription(deadLetterSubscriptionName)
      dlqSubscription = dlqSub

      let dlqMessage: PubSubMessageType | undefined
      dlqSubscription.on('message', (message: PubSubMessageType) => {
        dlqMessage = message
        message.ack()
      })

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '2',
        messageType: 'remove',
        timestamp: new Date(Date.now() - 1000).toISOString(),
        userIds: [],
      }
      await permissionPublisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('2', 'error')
      expect(spyResult.message).toEqual(message)
      // Due to exponential backoff and timestamp, message is retried a few times before being moved to DLQ
      expect(counter).toBeGreaterThanOrEqual(2)

      await waitAndRetry(() => dlqMessage, 50, 40)
      const messageBody = JSON.parse(dlqMessage!.data.toString())
      expect(messageBody).toMatchObject({
        id: '2',
        messageType: 'remove',
        timestamp: message.timestamp,
      })
    }, 10000)
  })
})
