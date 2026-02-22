import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { waitAndRetry } from '@lokalise/node-core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { assertQueue, deleteQueue, FakeConsumerErrorResolver } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { deserializeSNSMessage } from '../../utils/snsMessageDeserializer.ts'
import { subscribeToTopic } from '../../utils/snsSubscriber.ts'
import { assertTopic, deleteTopic } from '../../utils/snsUtils.ts'
import { SnsSqsPermissionConsumer } from '../../../test/consumers/SnsSqsPermissionConsumer.ts'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../../../test/consumers/userConsumerSchemas.ts'
import { SnsPermissionPublisher } from '../../../test/publishers/SnsPermissionPublisher.ts'
import type { Dependencies } from '../../../test/utils/testContext.ts'
import { registerDependencies } from '../../../test/utils/testContext.ts'
import { TestSnsPublisher } from './TestSnsPublisher.ts'

const queueName = 'test-sns-publisher-queue'
const topicName = 'test-sns-publisher-topic'
const fifoTopicName = 'test-sns-publisher-topic.fifo'
const fifoQueueName = 'test-sns-publisher-queue.fifo'

describe('TestSnsPublisher', () => {
  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient
  let publisher: TestSnsPublisher

  beforeEach(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
    publisher = new TestSnsPublisher(snsClient, stsClient)

    await deleteQueue(sqsClient, queueName)
    await deleteQueue(sqsClient, fifoQueueName)
    await deleteTopic(snsClient, stsClient, topicName)
    await deleteTopic(snsClient, stsClient, fifoTopicName)
  })

  afterEach(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('publish with topicArn', () => {
    it('publishes arbitrary message without validation', async () => {
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: queueName },
        { Name: topicName },
        { updateAttributesIfExists: false },
      )

      await publisher.publish(
        { totally: 'arbitrary', data: { nested: true }, number: 42 },
        { topicArn },
      )

      const receivedMessages: SQSMessage[] = []
      const consumer = Consumer.create({
        queueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      consumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({
        totally: 'arbitrary',
        data: { nested: true },
        number: 42,
      })
    })

    it('publishes message without required schema fields', async () => {
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: queueName },
        { Name: topicName },
        { updateAttributesIfExists: false },
      )

      await publisher.publish({ incomplete: 'message' }, { topicArn })

      const receivedMessages: SQSMessage[] = []
      const consumer = Consumer.create({
        queueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      consumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({ incomplete: 'message' })
    })
  })

  describe('publish with topicName', () => {
    it('publishes message using topic name', async () => {
      await assertTopic(snsClient, stsClient, { Name: topicName })
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: queueName },
        { Name: topicName },
        { updateAttributesIfExists: false },
      )

      await publisher.publish({ test: 'topicName' }, { topicName })

      const receivedMessages: SQSMessage[] = []
      const consumer = Consumer.create({
        queueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      consumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({ test: 'topicName' })
    })
  })

  describe('publish with publisher', () => {
    it('publishes to topic extracted from publisher', async () => {
      const regularPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
        },
      })
      await regularPublisher.init()

      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: queueName },
        { Name: topicName },
        { updateAttributesIfExists: false },
      )

      await publisher.publish({ test: 'publisher' }, { publisher: regularPublisher })

      const receivedMessages: SQSMessage[] = []
      const consumer = Consumer.create({
        queueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      consumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({ test: 'publisher' })

      await regularPublisher.close()
    })

    it('throws error when publisher is not initialized', async () => {
      const regularPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
        },
      })

      await expect(
        publisher.publish({ test: 'data' }, { publisher: regularPublisher }),
      ).rejects.toThrow('Publisher has not been initialized')
    })
  })

  describe('publish with consumer', () => {
    it('publishes to topic extracted from consumer', async () => {
      const consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
          topic: { Name: topicName },
        },
      })
      await consumer.init()

      // Use a separate queue to verify the message
      const verifyQueueName = 'test-sns-publisher-verify-queue'
      await deleteQueue(sqsClient, verifyQueueName)
      const { queueUrl: verifyQueueUrl } = await assertQueue(sqsClient, {
        QueueName: verifyQueueName,
      })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: verifyQueueName },
        { Name: topicName },
        { updateAttributesIfExists: false },
      )

      await publisher.publish({ test: 'consumer' }, { consumer })

      const receivedMessages: SQSMessage[] = []
      const sqsConsumer = Consumer.create({
        queueUrl: verifyQueueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      sqsConsumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      sqsConsumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({ test: 'consumer' })

      await consumer.close()
      await deleteQueue(sqsClient, verifyQueueName)
    })

    it('throws error when consumer is not initialized', async () => {
      const consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
          topic: { Name: topicName },
        },
      })

      await expect(publisher.publish({ test: 'data' }, { consumer })).rejects.toThrow(
        'Consumer has not been initialized',
      )
    })
  })

  describe('publish to FIFO topic', () => {
    it('publishes to FIFO topic with MessageGroupId and MessageDeduplicationId', async () => {
      const topicArn = await assertTopic(snsClient, stsClient, {
        Name: fifoTopicName,
        Attributes: { FifoTopic: 'true', ContentBasedDeduplication: 'false' },
      })
      const { queueUrl: fifoQueueUrl } = await assertQueue(sqsClient, {
        QueueName: fifoQueueName,
        Attributes: { FifoQueue: 'true' },
      })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
        { QueueName: fifoQueueName, Attributes: { FifoQueue: 'true' } },
        { Name: fifoTopicName, Attributes: { FifoTopic: 'true' } },
        { updateAttributesIfExists: false },
      )

      await publisher.publish(
        { test: 'fifo-message' },
        {
          topicArn,
          MessageGroupId: 'test-group',
          MessageDeduplicationId: 'unique-id-123',
        },
      )

      const receivedMessages: SQSMessage[] = []
      const consumer = Consumer.create({
        queueUrl: fifoQueueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })
      consumer.start()

      await waitAndRetry(() => receivedMessages.length > 0)
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const snsEnvelope = JSON.parse(receivedMessages[0]!.Body!)
      const body = JSON.parse(snsEnvelope.Message)
      expect(body).toEqual({ test: 'fifo-message' })
    })
  })

  describe('integration with consumer', () => {
    it('publishes a valid message that consumer can process', async () => {
      const consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
          topic: { Name: topicName },
        },
      })
      await consumer.start()

      // Publish a message matching the consumer's expected schema
      await publisher.publish(
        {
          id: 'test-integration-1',
          messageType: 'add',
        },
        { consumer },
      )

      const result = await consumer.handlerSpy.waitForMessageWithId(
        'test-integration-1',
        'consumed',
      )
      expect(result.message).toMatchObject({
        id: 'test-integration-1',
        messageType: 'add',
      })

      await consumer.close()
    })
  })

  describe('error handling', () => {
    it('throws error when no topic specified', async () => {
      await expect(
        // @ts-expect-error - Testing invalid input
        publisher.publish({ test: 'data' }, {}),
      ).rejects.toThrow('Either topicArn, topicName, consumer, or publisher must be provided')
    })
  })
})
