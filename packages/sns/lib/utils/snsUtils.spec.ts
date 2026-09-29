import {
  ListSubscriptionsByTopicCommand,
  type SNSClient,
  SubscribeCommand,
} from '@aws-sdk/client-sns'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { TestAwsResourceAdmin } from '../../test/utils/testAdmin.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import { findSubscriptionByTopicAndQueue } from './snsUtils.ts'

const TOPIC_NAME = 'topic'
const QUEUE_NAME = 'queue'

describe('snsUtils', () => {
  let diContainer: AwilixContainer<Dependencies>
  let snsClient: SNSClient
  let testAdmin: TestAwsResourceAdmin

  beforeEach(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
    testAdmin = diContainer.cradle.testAdmin
  })

  afterEach(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()

    await testAdmin.deleteQueues(QUEUE_NAME)
    await testAdmin.deleteTopics(TOPIC_NAME)
  })

  describe('findSubscriptionByTopicAndQueue', () => {
    it('returns the subscription of the queue to the topic', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)

      expect(subscription).toMatchObject({
        SubscriptionArn: subscriptionArn,
        TopicArn: topicArn,
        Protocol: 'sqs',
        Endpoint: queueArn,
      })
    })

    it('ignores subscriptions of the endpoint with a protocol other than sqs', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      await snsClient.send(
        new SubscribeCommand({ TopicArn: topicArn, Protocol: 'lambda', Endpoint: queueArn }),
      )

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)

      expect(subscription).toBeUndefined()
    })

    it('returns undefined when the queue is not subscribed to the topic', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)

      expect(subscription).toBeUndefined()
    })

    it('looks up the subscription in the following pages', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const snsSpy = vi.spyOn(snsClient, 'send').mockImplementation((command) => {
        // Second page contains the subscription
        if (command instanceof ListSubscriptionsByTopicCommand && command.input.NextToken) {
          return Promise.resolve({
            Subscriptions: [
              { Protocol: 'sqs', Endpoint: queueArn, SubscriptionArn: 'subscription' },
            ],
          })
        }
        // First page does not contain the subscription and points to the next one
        return Promise.resolve({
          Subscriptions: [
            {
              Protocol: 'sqs',
              Endpoint: `${queueArn}-other`,
              SubscriptionArn: 'other-subscription',
            },
          ],
          NextToken: 'next-page',
        })
      })

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)

      expect(subscription?.SubscriptionArn).toBe('subscription')
      expect(snsSpy).toHaveBeenCalledTimes(2)
      expect(snsSpy.mock.calls[1]![0].input).toMatchObject({
        TopicArn: topicArn,
        NextToken: 'next-page',
      })
    })
  })
})
