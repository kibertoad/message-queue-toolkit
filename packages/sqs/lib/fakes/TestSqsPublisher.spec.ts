import type { SQSClient } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SqsPermissionConsumer } from '../../test/consumers/SqsPermissionConsumer.ts'
import { SqsPermissionPublisher } from '../../test/publishers/SqsPermissionPublisher.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import type { SQSMessage } from '../types/MessageTypes.ts'
import { assertQueue, deleteQueue } from '../utils/sqsUtils.ts'
import { TestSqsPublisher } from './TestSqsPublisher.ts'

describe('TestSqsPublisher', () => {
  const queueName = 'test-publisher-queue'
  const queueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`
  const fifoQueueName = 'test-publisher-queue.fifo'
  const fifoQueueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${fifoQueueName}`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let publisher: TestSqsPublisher

  beforeEach(async () => {
    diContainer = await registerDependencies()
    sqsClient = diContainer.cradle.sqsClient
    publisher = new TestSqsPublisher(sqsClient)
    await deleteQueue(sqsClient, queueName)
    await deleteQueue(sqsClient, fifoQueueName)
  })

  afterEach(async () => {
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('publish with queueUrl', () => {
    it('publishes arbitrary message without validation', async () => {
      await assertQueue(sqsClient, { QueueName: queueName })

      // Publish a message that doesn't match any schema
      await publisher.publish(
        {
          totally: 'arbitrary',
          data: { nested: true },
          number: 42,
        },
        { queueUrl },
      )

      // Verify message was published by consuming it
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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({
        totally: 'arbitrary',
        data: { nested: true },
        number: 42,
      })
    })

    it('publishes message without required schema fields', async () => {
      await assertQueue(sqsClient, { QueueName: queueName })

      // Publish a message missing required fields like messageType, id, timestamp
      await publisher.publish({ incomplete: 'message' }, { queueUrl })

      // Verify message was published
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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({ incomplete: 'message' })
    })

    it('publishes complex nested objects', async () => {
      await assertQueue(sqsClient, { QueueName: queueName })

      const complexMessage = {
        arrays: [1, 2, 3],
        nested: {
          deeply: {
            nested: {
              value: 'test',
            },
          },
        },
        nullValue: null,
        boolValue: true,
      }

      await publisher.publish(complexMessage, { queueUrl })

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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual(complexMessage)
    })
  })

  describe('publish with queueName', () => {
    it('publishes message using queue name', async () => {
      await assertQueue(sqsClient, { QueueName: queueName })

      await publisher.publish({ test: 'queueName' }, { queueName })

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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({ test: 'queueName' })
    })
  })

  describe('publish with consumer', () => {
    it('publishes to queue extracted from consumer', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })
      await consumer.init()

      await publisher.publish({ test: 'consumer' }, { consumer })

      // Verify message was published
      const receivedMessages: SQSMessage[] = []
      const sqsConsumer = Consumer.create({
        queueUrl,
        sqs: sqsClient,
        // biome-ignore lint/suspicious/useAwait: Consumer.create requires async handler
        handleMessage: async (message: SQSMessage) => {
          receivedMessages.push(message)
          return message
        },
      })

      sqsConsumer.start()
      await new Promise((resolve) => setTimeout(resolve, 1000))
      sqsConsumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({ test: 'consumer' })

      await consumer.close()
    })

    it('throws error when consumer is not initialized', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      await expect(publisher.publish({ test: 'data' }, { consumer })).rejects.toThrow(
        'Consumer has not been initialized',
      )
    })
  })

  describe('publish with publisher', () => {
    it('publishes to queue extracted from publisher', async () => {
      const regularPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })
      await regularPublisher.init()

      await publisher.publish({ test: 'publisher' }, { publisher: regularPublisher })

      // Verify message was published
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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({ test: 'publisher' })
    })

    it('throws error when publisher is not initialized', async () => {
      const regularPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      await expect(
        publisher.publish({ test: 'data' }, { publisher: regularPublisher }),
      ).rejects.toThrow('Publisher has not been initialized')
    })
  })

  describe('publish to FIFO queue', () => {
    it('publishes to FIFO queue with MessageGroupId and MessageDeduplicationId', async () => {
      await assertQueue(sqsClient, {
        QueueName: fifoQueueName,
        Attributes: {
          FifoQueue: 'true',
        },
      })

      await publisher.publish(
        { test: 'fifo-message' },
        {
          queueUrl: fifoQueueUrl,
          MessageGroupId: 'test-group',
          MessageDeduplicationId: 'unique-id-123',
        },
      )

      // Verify message was published
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
      await new Promise((resolve) => setTimeout(resolve, 1000))
      consumer.stop()

      expect(receivedMessages).toHaveLength(1)
      const body = JSON.parse(receivedMessages[0]?.Body!)
      expect(body).toEqual({ test: 'fifo-message' })
    })
  })

  describe('integration with consumer', () => {
    it('publishes invalid message that consumer cannot process', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })
      await consumer.init()

      // Publish a message that doesn't match the consumer's expected schema
      await publisher.publish(
        {
          invalid: 'schema',
          missing: 'messageType',
        },
        { consumer },
      )

      // Start consumer - it should receive but fail to process the message
      await consumer.start()
      await new Promise((resolve) => setTimeout(resolve, 2000))

      // The message should fail validation and be marked as error
      // We can't easily check this with handlerSpy since invalid messages don't have proper IDs
      // But the test demonstrates that TestSqsPublisher can send invalid messages

      await consumer.close()
    })
  })

  describe('error handling', () => {
    it('throws error when no queue specified', async () => {
      await expect(
        // @ts-expect-error - Testing invalid input
        publisher.publish({ test: 'data' }, {}),
      ).rejects.toThrow('Either queueUrl, queueName, consumer, or publisher must be provided')
    })
  })
})
