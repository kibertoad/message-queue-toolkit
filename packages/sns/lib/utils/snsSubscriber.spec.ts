import {
  SetSubscriptionAttributesCommand,
  type SNSClient,
  SubscribeCommand,
} from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { FakeLogger } from '../../test/fakes/FakeLogger.ts'
import { isLocalstack } from '../../test/utils/fauxqsInstance.ts'
import type { TestAwsResourceAdmin } from '../../test/utils/testAdmin.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import { assertSubscription, setSubscriptionAttributes, subscribeToTopic } from './snsSubscriber.ts'
import { findSubscriptionByTopicAndQueue, getSubscriptionAttributes } from './snsUtils.ts'

const TOPIC_NAME = 'topic'
const QUEUE_NAME = 'queue'

describe('snsSubscriber', () => {
  let diContainer: AwilixContainer<Dependencies>
  let snsClient: SNSClient
  let sqsClient: SQSClient
  let stsClient: STSClient
  let testAdmin: TestAwsResourceAdmin

  beforeEach(async () => {
    diContainer = await registerDependencies({}, false)
    snsClient = diContainer.cradle.snsClient
    sqsClient = diContainer.cradle.sqsClient
    stsClient = diContainer.cradle.stsClient
    testAdmin = diContainer.cradle.testAdmin
  })

  afterEach(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()

    await testAdmin.deleteQueues(QUEUE_NAME)
    await testAdmin.deleteTopics(TOPIC_NAME)
  })

  describe('subscribeToTopic', () => {
    it('logs queue in subscription error', async () => {
      const logger = new FakeLogger()
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
          stsClient,
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
      ).rejects.toThrow(/Subscription already exists with different attributes/)

      expect(logger.loggedErrors).toHaveLength(1)
      expect(logger.loggedErrors[0]).toBe(
        'Error while creating subscription for queue "queue", topic "topic": Subscription already exists with different attributes: FilterPolicy, FilterPolicyScope',
      )
    })

    it('updates conflicting subscription', async () => {
      const logger = new FakeLogger()
      const subscription = await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
        stsClient,
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

  describe('assertSubscription', () => {
    it('subscribes an existing queue to an existing topic', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)

      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        { updateAttributesIfExists: false },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
      expect(subscriptionArn).toBe(subscription?.SubscriptionArn)
    })

    it('returns the existing subscription when it already exists with same attributes', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const existingSubscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)

      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        { updateAttributesIfExists: false },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      expect(subscriptionArn).toBe(existingSubscriptionArn)
    })

    it('updates attributes of the existing subscription when they are different', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const existingSubscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: {
            FilterPolicy: `{"type":["remove"]}`,
            FilterPolicyScope: 'MessageAttributes',
          },
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: { FilterPolicy: `{"type":["add"]}`, FilterPolicyScope: 'MessageBody' },
          updateAttributesIfExists: true,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
        new FakeLogger(),
      )

      expect(subscriptionArn).toBe(existingSubscriptionArn)
      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)
      expect(subscriptionAttributes.result?.attributes).toMatchObject({
        FilterPolicy: `{"type":["add"]}`,
        FilterPolicyScope: 'MessageBody',
      })
    })

    it('does not write to an existing subscription when its attributes are unchanged', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionConfig = {
        Attributes: {
          FilterPolicy: `{"type":["add","remove"]}`,
          FilterPolicyScope: 'MessageBody',
          RawMessageDelivery: 'true',
        },
        updateAttributesIfExists: true,
      }
      const existingSubscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        subscriptionConfig,
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )
      const sendSpy = vi.spyOn(snsClient, 'send')

      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          ...subscriptionConfig,
          Attributes: {
            ...subscriptionConfig.Attributes,
            FilterPolicy: `{ "type": [ "add", "remove" ] }`,
          },
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      expect(subscriptionArn).toBe(existingSubscriptionArn)
      const sentCommands = sendSpy.mock.calls.map(([command]) => command)
      expect(sentCommands.some((command) => command instanceof SubscribeCommand)).toBe(false)
      expect(
        sentCommands.some((command) => command instanceof SetSubscriptionAttributesCommand),
      ).toBe(false)
    })

    it('writes only the attributes that changed', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: { FilterPolicy: `{"type":["remove"]}`, RawMessageDelivery: 'true' },
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )
      const sendSpy = vi.spyOn(snsClient, 'send')

      await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: { FilterPolicy: `{"type":["add"]}`, RawMessageDelivery: 'true' },
          updateAttributesIfExists: true,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
        new FakeLogger(),
      )

      const writtenAttributes = sendSpy.mock.calls
        .map(([command]) => command)
        .filter((command) => command instanceof SetSubscriptionAttributesCommand)
        .map((command) => command.input.AttributeName)
      expect(writtenAttributes).toEqual(['FilterPolicy'])
      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)
      expect(subscriptionAttributes.result?.attributes?.FilterPolicy).toBe(`{"type":["add"]}`)
    })

    it('neither checks nor writes attributes that are not managed', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: { FilterPolicy: `{"type":["add"]}`, RawMessageDelivery: 'true' },
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )
      const sendSpy = vi.spyOn(snsClient, 'send')

      await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: { FilterPolicy: `{"type":["add"]}` },
          managedAttributes: ['FilterPolicy', 'FilterPolicyScope'],
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      expect(
        sendSpy.mock.calls.some(([command]) => command instanceof SetSubscriptionAttributesCommand),
      ).toBe(false)
      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)
      expect(subscriptionAttributes.result?.attributes?.RawMessageDelivery).toBe('true')
    })

    it('resets managed attributes that were removed from the config', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionArn = await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: {
            FilterPolicy: `{"type":["add"]}`,
            FilterPolicyScope: 'MessageBody',
            RawMessageDelivery: 'true',
          },
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        { Attributes: { FilterPolicy: `{"type":["add"]}` }, updateAttributesIfExists: true },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
        new FakeLogger(),
      )

      const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)
      expect(subscriptionAttributes.result?.attributes).toMatchObject({
        FilterPolicy: `{"type":["add"]}`,
        FilterPolicyScope: 'MessageAttributes',
        RawMessageDelivery: 'false',
      })

      // A subscription that is already reset is not written to again
      const sendSpy = vi.spyOn(snsClient, 'send')
      await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        { Attributes: { FilterPolicy: `{"type":["add"]}` }, updateAttributesIfExists: true },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )
      expect(
        sendSpy.mock.calls.some(([command]) => command instanceof SetSubscriptionAttributesCommand),
      ).toBe(false)
    })

    // LocalStack cannot remove a RedrivePolicy: it rejects both an empty and an omitted value
    it.skipIf(isLocalstack)(
      'removes a managed redrive policy missing from the config',
      async () => {
        const topicArn = await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
        const subscriptionArn = await assertSubscription(
          snsClient,
          topicArn,
          queueArn,
          {
            Attributes: { RedrivePolicy: JSON.stringify({ deadLetterTargetArn: queueArn }) },
            updateAttributesIfExists: false,
          },
          { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
        )
        const sendSpy = vi.spyOn(snsClient, 'send')

        await assertSubscription(
          snsClient,
          topicArn,
          queueArn,
          { updateAttributesIfExists: true },
          { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
          new FakeLogger(),
        )

        const redrivePolicyWrite = sendSpy.mock.calls
          .map(([command]) => command)
          .find(
            (command) =>
              command instanceof SetSubscriptionAttributesCommand &&
              command.input.AttributeName === 'RedrivePolicy',
          )
        expect(redrivePolicyWrite?.input).toEqual({
          SubscriptionArn: subscriptionArn,
          AttributeName: 'RedrivePolicy',
          AttributeValue: undefined,
        })
        const subscriptionAttributes = await getSubscriptionAttributes(snsClient, subscriptionArn!)
        expect(
          subscriptionAttributes.result?.attributes?.RedrivePolicy || undefined,
        ).toBeUndefined()
      },
    )

    it('throws when a configured attribute is not managed', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)

      await expect(
        assertSubscription(
          snsClient,
          topicArn,
          queueArn,
          {
            Attributes: { FilterPolicy: `{"type":["add"]}`, RawMessageDelivery: 'true' },
            managedAttributes: ['FilterPolicy'],
            updateAttributesIfExists: true,
          },
          { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
        ),
      ).rejects.toThrow(/RawMessageDelivery are set, but not listed in managedAttributes/)
    })

    it('throws when attributes are different and update is disabled', async () => {
      const logger = new FakeLogger()
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      await assertSubscription(
        snsClient,
        topicArn,
        queueArn,
        {
          Attributes: {
            FilterPolicy: `{"type":["remove"]}`,
            FilterPolicyScope: 'MessageAttributes',
          },
          updateAttributesIfExists: false,
        },
        { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
      )

      await expect(
        assertSubscription(
          snsClient,
          topicArn,
          queueArn,
          {
            Attributes: { FilterPolicy: `{"type":["add"]}`, FilterPolicyScope: 'MessageBody' },
            updateAttributesIfExists: false,
          },
          { queueName: QUEUE_NAME, topicName: TOPIC_NAME },
          logger,
        ),
      ).rejects.toThrow(/Subscription already exists with different attributes/)
      expect(logger.loggedErrors[0]).toMatch(
        /^Error while creating subscription for queue "queue", topic "topic"/,
      )
    })
  })

  describe('setSubscriptionAttributes', () => {
    it('writes only the attributes that differ from the current ones', async () => {
      const topicArn = await testAdmin.createTopic(TOPIC_NAME)
      const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
      const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)
      await setSubscriptionAttributes(snsClient, subscriptionArn, {
        FilterPolicy: `{"type":["add"]}`,
      })
      const sendSpy = vi.spyOn(snsClient, 'send')

      await setSubscriptionAttributes(
        snsClient,
        subscriptionArn,
        { FilterPolicy: `{ "type": ["add"] }`, RawMessageDelivery: 'true' },
        ['FilterPolicy', 'RawMessageDelivery'],
      )

      const writtenAttributes = sendSpy.mock.calls
        .map(([command]) => command)
        .filter((command) => command instanceof SetSubscriptionAttributesCommand)
        .map((command) => command.input.AttributeName)
      expect(writtenAttributes).toEqual(['RawMessageDelivery'])
    })
  })
})
