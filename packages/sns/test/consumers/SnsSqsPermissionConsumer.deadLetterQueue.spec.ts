import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  type SQSMessage,
} from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { deleteTopic } from '../../lib/utils/snsUtils'
import type { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer'
import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'

// Note that dead letter queue are fully tested by sqs library - only including a few tests here to make sure the integration works
describe('SnsSqsPermissionConsumer - dead letter queue', () => {
  const topicName = SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME
  const queueName = SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME
  const deadLetterQueueName = `${queueName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient

  let publisher: SnsPermissionPublisher
  let consumer: SnsSqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    publisher = diContainer.cradle.permissionPublisher
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await deleteQueue(sqsClient, deadLetterQueueName)
    await deleteTopic(snsClient, topicName)
  })

  afterEach(async () => {
    await consumer?.close()
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('init', () => {
    it('creates a new dead letter queue', async () => {
      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: 'sometopic' },
          queue: { QueueName: 'existingQueue' },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: {
            queue: { QueueName: 'deadLetterQueue' },
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:deadLetterQueue`,
          maxReceiveCount: 3,
        }),
      })
    })

    it('using existing dead letter queue', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'deadLetterQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: 'sometopic' },
          queue: { QueueName: 'existingQueue' },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          locatorConfig: {
            queueUrl: 'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:deadLetterQueue`,
          maxReceiveCount: 3,
        }),
      })
    })
  })

  describe('messages are sent to DLQ', () => {
    it('Stuck messages are sent to DLQ', async () => {
      let counter = 0
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName }, topic: { Name: topicName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 200 },
        },
        maxRetryDuration: 3,
        removeHandlerOverride: async () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.subscriptionProps.deadLetterQueueUrl ?? '',
        handleMessage: async (message: SQSMessage) => {
          dlqMessage = message
        },
      })
      dlqConsumer.start()

      const message: PERMISSIONS_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        userIds: [1],
        permissions: ['100'],
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
      }
      await publisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spyResult.message).toEqual(message)
      expect(counter).toBeGreaterThan(2)

      await waitAndRetry(async () => dlqMessage)
      expect(JSON.parse(dlqMessage.Body)).toMatchObject({ id: '1', messageType: 'remove' })

      dlqConsumer.stop()
    })
  })
})
