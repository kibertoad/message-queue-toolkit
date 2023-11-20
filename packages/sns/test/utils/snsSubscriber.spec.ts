import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, describe, expect } from 'vitest'

import { subscribeToTopic } from '../../lib/utils/snsSubscriber'
import {
  deleteTopic,
  findSubscriptionByTopicAndQueue,
  getSubscriptionAttributes,
} from '../../lib/utils/snsUtils'
import { FakeLogger } from '../fakes/FakeLogger'

import type { Dependencies } from './testContext'
import { registerDependencies } from './testContext'

const TOPIC_NAME = 'topic'
const QUEUE_NAME = 'queue'

describe('snsSubscriber', () => {
  let diContainer: AwilixContainer<Dependencies>
  let snsClient: SNSClient
  let sqsClient: SQSClient
  beforeEach(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
    sqsClient = diContainer.cradle.sqsClient
  })

  afterEach(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()

    await deleteTopic(snsClient, TOPIC_NAME)
    await deleteQueue(sqsClient, QUEUE_NAME, true)
  })

  describe('subscribeToTopic', () => {
    it('logs queue in subscription error', async () => {
      const logger = new FakeLogger()
      await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: QUEUE_NAME,
        },
        {
          Name: TOPIC_NAME,
        },
        {
          Attributes: {
            FilterPolicy: `{"type":["remove"]}`,
            FilterPolicyScope: 'MessageAttributes',
          },
          updateAttributesIfExists: false,
        },
      )

      await expect(
        subscribeToTopic(
          sqsClient,
          snsClient,
          {
            QueueName: QUEUE_NAME,
          },
          {
            Name: TOPIC_NAME,
          },
          {
            Attributes: {
              FilterPolicy: `{"type":["add"]}`,
              FilterPolicyScope: 'MessageBody',
            },
            updateAttributesIfExists: false,
          },
          {
            logger,
          },
        ),
      ).rejects.toThrow(
        /Invalid parameter: Attributes Reason: Subscription already exists with different attributes/,
      )

      expect(logger.loggedErrors).toHaveLength(1)
      expect(logger.loggedErrors[0]).toBe(
        'Error while creating subscription for queue "queue", topic "topic": Invalid parameter: Attributes Reason: Subscription already exists with different attributes',
      )
    })

    it('updates conflicting subscription', async () => {
      const logger = new FakeLogger()
      const subscription = await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: QUEUE_NAME,
        },
        {
          Name: TOPIC_NAME,
        },
        {
          Attributes: {
            FilterPolicy: `{"type":["remove"]}`,
            FilterPolicyScope: 'MessageAttributes',
          },
          updateAttributesIfExists: false,
        },
      )

      await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: QUEUE_NAME,
        },
        {
          Name: TOPIC_NAME,
        },
        {
          Attributes: {
            FilterPolicy: `{"type":["add"]}`,
            FilterPolicyScope: 'MessageBody',
          },
          updateAttributesIfExists: true,
        },
        {
          logger,
        },
      )

      const updatedSubscription = await findSubscriptionByTopicAndQueue(
        snsClient,
        subscription.topicArn,
        subscription.queueArn,
      )

      const subscriptionAttributes = await getSubscriptionAttributes(
        snsClient,
        updatedSubscription!.SubscriptionArn!,
      )
      expect(subscriptionAttributes).toEqual({
        result: {
          attributes: {
            ConfirmationWasAuthenticated: 'true',
            Endpoint: subscription.queueArn,
            FilterPolicy: `{"type":["add"]}`,
            FilterPolicyScope: 'MessageBody',
            Owner: '000000000000',
            PendingConfirmation: 'false',
            Protocol: 'sqs',
            RawMessageDelivery: 'false',
            SubscriptionArn: expect.any(String),
            SubscriptionPrincipal: expect.any(String),
            TopicArn: subscription.topicArn,
          },
        },
      })
    })
  })
})
