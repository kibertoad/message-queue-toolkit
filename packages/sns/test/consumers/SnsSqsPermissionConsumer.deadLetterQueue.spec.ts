import { ListQueueTagsCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import { assertQueue, getQueueAttributes, type SQSMessage } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import type { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer.ts'
import type { PERMISSIONS_REMOVE_MESSAGE_TYPE } from './userConsumerSchemas.ts'

// Note that dead letter queue are fully tested by sqs library - only including a few tests here to make sure the integration works
describe('SnsSqsPermissionConsumer - dead letter queue', () => {
  const topicName = SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME
  const queueName = SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME
  const deadLetterQueueName = `${queueName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let testAdmin: TestAwsResourceAdmin

  let publisher: SnsPermissionPublisher
  let consumer: SnsSqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    testAdmin = diContainer.cradle.testAdmin
    publisher = diContainer.cradle.permissionPublisher
  })

  beforeEach(async () => {
    await testAdmin.deleteQueue(queueName)
    await testAdmin.deleteQueue(deadLetterQueueName)
    await testAdmin.deleteTopic(topicName)
  })

  afterEach(async () => {
    await consumer?.close()
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('init', () => {
    const topicName = 'sometopic'
    const queueName = 'myQueue'
    const deadLetterQueueName = 'deadLetterQueue'

    beforeEach(async () => {
      await testAdmin.deleteQueue(queueName)
      await testAdmin.deleteQueue(deadLetterQueueName)
      await testAdmin.deleteTopic(topicName)
    })

    it('creates a new dead letter queue', async () => {
      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: {
            queue: { QueueName: deadLetterQueueName },
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
      )

      const attributes = await getQueueAttributes(sqsClient, newConsumer.subscriptionProps.queueUrl)

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
          maxReceiveCount: 3,
        }),
      })
    })

    it('using existing dead letter queue', async () => {
      await assertQueue(sqsClient, {
        QueueName: deadLetterQueueName,
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          locatorConfig: {
            queueUrl: `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
      )

      const attributes = await getQueueAttributes(sqsClient, newConsumer.subscriptionProps.queueUrl)

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
          maxReceiveCount: 3,
        }),
      })
    })

    it('should update attributes and tags', async () => {
      await assertQueue(sqsClient, {
        QueueName: deadLetterQueueName,
        Attributes: { KmsMasterKeyId: 'old' },
        tags: { tag: 'old' },
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: {
            forceTagUpdate: true,
            updateAttributesIfExists: true,
            queue: {
              QueueName: deadLetterQueueName,
              Attributes: { KmsMasterKeyId: 'new' },
              tags: { tag: 'new' },
            },
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
      )

      const mainQueueAttributes = await getQueueAttributes(
        sqsClient,
        newConsumer.subscriptionProps.queueUrl,
      )
      expect(mainQueueAttributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
          maxReceiveCount: 3,
        }),
      })

      const dlqAttributes = await getQueueAttributes(
        sqsClient,
        newConsumer.subscriptionProps.deadLetterQueueUrl!,
      )
      expect(dlqAttributes.result?.attributes).toMatchObject({
        KmsMasterKeyId: 'new',
      })

      const tags = await sqsClient.send(
        new ListQueueTagsCommand({ QueueUrl: newConsumer.subscriptionProps.deadLetterQueueUrl }),
      )
      expect(tags.Tags).toMatchInlineSnapshot(`
          {
            "tag": "new",
          }
      `)
    })
  })

  describe('messages are sent to DLQ', () => {
    it('Stuck messages are sent to DLQ', async () => {
      let counter = 0
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
          topic: { Name: topicName },
        },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 200 },
        },
        maxRetryDuration: 3,
        removeHandlerOverride: () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.subscriptionProps.deadLetterQueueUrl ?? '',
        handleMessage: (message: SQSMessage) => {
          dlqMessage = message
          return Promise.resolve(message)
        },
      })
      dlqConsumer.start()

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        timestamp: new Date(Date.now() - 2 * 1000).toISOString(),
      }
      await publisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spyResult.message).toEqual(message)
      // due to exponential backoff and timestamp, message is only retried once before being moved to DLQ
      expect(counter).toBe(2)

      await waitAndRetry(async () => dlqMessage)

      const messageBody = JSON.parse(dlqMessage.Body)
      expect(messageBody).toEqual({
        id: '1',
        messageType: 'remove',
        timestamp: message.timestamp,
        _internalRetryLaterCount: 1,
      })

      dlqConsumer.stop()
    })
  })
})
