import type { SNSClient } from '@aws-sdk/client-sns'
import { ListQueueTagsCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import {
  type SQSMessage,
  assertQueue,
  deleteQueue,
  getQueueAttributes,
} from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { deleteTopic } from '../../lib/utils/snsUtils'
import type { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import type { STSClient } from '@aws-sdk/client-sts'
import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer'
import type { PERMISSIONS_REMOVE_MESSAGE_TYPE } from './userConsumerSchemas'

// Note that dead letter queue are fully tested by sqs library - only including a few tests here to make sure the integration works
describe('SnsSqsPermissionConsumer - dead letter queue', () => {
  const topicName = SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME
  const queueName = SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME
  const deadLetterQueueName = `${queueName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient

  let publisher: SnsPermissionPublisher
  let consumer: SnsSqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
    publisher = diContainer.cradle.permissionPublisher
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await deleteQueue(sqsClient, deadLetterQueueName)
    await deleteTopic(snsClient, stsClient, topicName)
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
      await deleteQueue(sqsClient, queueName)
      await deleteQueue(sqsClient, deadLetterQueueName)
      await deleteTopic(snsClient, stsClient, topicName)
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
          return Promise.resolve()
        },
      })
      dlqConsumer.start()

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
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
        _internalNumberOfRetries: expect.any(Number),
      })
      // due to exponential backoff and timestamp, on second retry message is moved to DLQ so _internalNumberOfRetries is 1
      expect(messageBody._internalNumberOfRetries).toBe(1)

      dlqConsumer.stop()
    })
  })
})
