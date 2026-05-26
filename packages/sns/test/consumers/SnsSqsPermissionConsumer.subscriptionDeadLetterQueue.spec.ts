import { PublishCommand, type SNSClient } from '@aws-sdk/client-sns'
import { ReceiveMessageCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { getSubscriptionAttributes } from '../../lib/utils/snsUtils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer.ts'

describe('SnsSqsPermissionConsumer - subscription dead letter queue', () => {
  const topicName = 'subscription-dlq-test-topic'
  const queueName = 'subscription-dlq-test-queue'
  const deadLetterQueueName = `${queueName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let snsClient: SNSClient
  let sqsClient: SQSClient
  let testAdmin: TestAwsResourceAdmin

  let consumer: SnsSqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
    sqsClient = diContainer.cradle.sqsClient
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.deleteQueues(queueName, deadLetterQueueName)
    await testAdmin.deleteTopics(topicName)
  })

  afterEach(async () => {
    await consumer?.close()
    consumer = undefined
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('construction validation', () => {
    it('throws when subscriptionDeadLetterQueue is set without deadLetterQueue', () => {
      expect(
        () =>
          new SnsSqsPermissionConsumer(diContainer.cradle, {
            creationConfig: {
              topic: { Name: topicName },
              queue: { QueueName: queueName },
            },
            subscriptionDeadLetterQueue: { reuseConsumerDeadLetterQueue: true },
          }),
      ).toThrow(
        'subscriptionDeadLetterQueue.reuseConsumerDeadLetterQueue requires deadLetterQueue to be configured',
      )
    })
  })

  describe('reuseConsumerDeadLetterQueue', () => {
    it('sets RedrivePolicy on the subscription pointing to the consumer DLQ', async () => {
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: {
            queue: { QueueName: deadLetterQueueName },
          },
        },
        subscriptionDeadLetterQueue: { reuseConsumerDeadLetterQueue: true },
      })

      await consumer.init()

      const { subscriptionArn } = consumer.subscriptionProps
      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)

      const dlqArn = `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`
      expect(subscriptionAttributes.result?.attributes?.RedrivePolicy).toBe(
        JSON.stringify({ deadLetterTargetArn: dlqArn }),
      )
    })

    it('does not set RedrivePolicy on the subscription when the option is not enabled', async () => {
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
        },
      })

      await consumer.init()

      const { subscriptionArn } = consumer.subscriptionProps
      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)

      expect(subscriptionAttributes.result?.attributes?.RedrivePolicy).toBeUndefined()
    })

    it('routes undeliverable messages to the consumer DLQ when the endpoint queue is gone', async () => {
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName },
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
        },
        subscriptionDeadLetterQueue: { reuseConsumerDeadLetterQueue: true },
      })

      await consumer.init()

      const {
        topicArn,
        queueName: sourceQueueName,
        deadLetterQueueUrl,
      } = consumer.subscriptionProps

      // Force a delivery failure: delete the endpoint queue while the
      // subscription stays in place. Subsequent SNS publishes can't reach it,
      // so the subscription RedrivePolicy should route the message to the DLQ.
      await testAdmin.deleteQueues(sourceQueueName!)

      await snsClient.send(
        new PublishCommand({
          TopicArn: topicArn,
          Message: JSON.stringify({
            id: 'sub-dlq-1',
            messageType: 'remove',
            timestamp: new Date().toISOString(),
          }),
        }),
      )

      let dlqBody: string | undefined
      const arrived = await waitAndRetry(
        async () => {
          const response = await sqsClient.send(
            new ReceiveMessageCommand({
              QueueUrl: deadLetterQueueUrl,
              MaxNumberOfMessages: 1,
              WaitTimeSeconds: 1,
            }),
          )
          dlqBody = response.Messages?.[0]?.Body
          return !!dlqBody
        },
        50,
        60,
      )

      expect(arrived).toBe(true)
      expect(dlqBody).toContain('sub-dlq-1')
    })
  })
})
