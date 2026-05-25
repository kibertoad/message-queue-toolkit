import type { SNSClient } from '@aws-sdk/client-sns'
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
  let testAdmin: TestAwsResourceAdmin

  let consumer: SnsSqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
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
  })
})
