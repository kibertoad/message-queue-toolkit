import { setTimeout } from 'node:timers/promises'
import {
  ListSubscriptionsByTopicCommand,
  type SNSClient,
  SubscribeCommand,
} from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getPort } from '../../test/utils/fauxqsInstance.ts'
import type { TestAwsResourceAdmin } from '../../test/utils/testAdmin.ts'
import type { Dependencies } from '../../test/utils/testContext.ts'
import { registerDependencies } from '../../test/utils/testContext.ts'
import { initSnsSqs } from './snsInitter.ts'
import { findSubscriptionByTopicAndQueue } from './snsUtils.ts'

const TOPIC_NAME = 'sns-initter-topic'
const QUEUE_NAME = 'sns-initter-queue'

describe('snsInitter', () => {
  const queueUrl = `http://sqs.eu-west-1.localstack:${getPort()}/000000000000/${QUEUE_NAME}`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient
  let testAdmin: TestAwsResourceAdmin

  beforeEach(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
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

  describe('initSnsSqs', () => {
    describe('resource resolution', () => {
      it('creates subscription when topic and queue are located', async () => {
        const topicArn = await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)

        const result = await initSnsSqs(
          sqsClient,
          snsClient,
          stsClient,
          { topicName: TOPIC_NAME, queueName: QUEUE_NAME },
          undefined,
          { updateAttributesIfExists: false },
        )

        const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
        expect(subscription).toBeDefined()
        expect(result).toEqual({
          topicArn,
          queueUrl,
          queueArn,
          queueName: QUEUE_NAME,
          subscriptionArn: subscription?.SubscriptionArn,
        })
      })

      it('locates existing subscription when subscription config is not provided', async () => {
        const topicArn = await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
        const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)
        const snsSpy = vi.spyOn(snsClient, 'send')

        const result = await initSnsSqs(sqsClient, snsClient, stsClient, {
          topicName: TOPIC_NAME,
          queueName: QUEUE_NAME,
        })

        expect(result).toEqual({
          topicArn,
          queueUrl,
          queueArn,
          queueName: QUEUE_NAME,
          subscriptionArn,
        })
        // Subscription is only located, never created
        expect(snsSpy).not.toHaveBeenCalledWith(expect.any(SubscribeCommand))
      })

      it('throws when subscription to locate does not exist', async () => {
        await testAdmin.createTopic(TOPIC_NAME)
        await testAdmin.createQueue(QUEUE_NAME)

        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, {
            topicName: TOPIC_NAME,
            queueName: QUEUE_NAME,
          }),
        ).rejects.toThrow(/Subscription of queue .* to topic .* does not exist/)
      })

      it('ignores subscription pending confirmation when locating it', async () => {
        await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
        const originalSend = snsClient.send.bind(snsClient)
        vi.spyOn(snsClient, 'send').mockImplementation((command) => {
          if (command instanceof ListSubscriptionsByTopicCommand) {
            return Promise.resolve({
              Subscriptions: [
                { Protocol: 'sqs', Endpoint: queueArn, SubscriptionArn: 'non-arn-like' },
              ],
            })
          }
          return originalSend(command)
        })

        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, {
            topicName: TOPIC_NAME,
            queueName: QUEUE_NAME,
          }),
        ).rejects.toThrow(/Subscription of queue .* to topic .* does not exist/)
      })
    })

    describe('config validation', () => {
      it('throws when neither topic creation config nor topic locator is provided', async () => {
        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, { queueName: QUEUE_NAME }, undefined, {
            updateAttributesIfExists: false,
          }),
        ).rejects.toThrow(
          /creationConfig.topic is mandatory .* OR locatorConfig.name or locatorConfig.topicArn parameter is mandatory/,
        )
      })

      it('throws when neither queue creation config nor queue locator is provided', async () => {
        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, { topicName: TOPIC_NAME }, undefined, {
            updateAttributesIfExists: false,
          }),
        ).rejects.toThrow(
          'Either creationConfig.queue is mandatory in order to create the queue, or locatorConfig.queueUrl or locatorConfig.queueName is mandatory in order to locate the existing queue',
        )
      })

      it('throws when queue creation config has no queue name', async () => {
        await expect(
          initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            { topicName: TOPIC_NAME },
            { queue: { QueueName: '' } },
            { updateAttributesIfExists: false },
          ),
        ).rejects.toThrow(/creationConfig.queue.QueueName parameter is mandatory/)
      })

      it('throws when queue creation config is provided without subscription config', async () => {
        await expect(
          initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            { topicName: TOPIC_NAME },
            { queue: { QueueName: QUEUE_NAME } },
          ),
        ).rejects.toThrow(
          'If creationConfig.queue is specified, subscriptionConfig is mandatory, as the subscription of a queue being created cannot be located',
        )
      })
    })

    describe('non-blocking startup resource polling', () => {
      const startupResourcePolling = {
        enabled: true,
        pollingIntervalMs: 50,
        timeoutMs: 5000,
        nonBlocking: true,
      }

      it('invokes onResourcesReady callback when located subscription becomes available', async () => {
        const topicArn = await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
        const onResourcesReady = vi.fn()

        const result = await initSnsSqs(
          sqsClient,
          snsClient,
          stsClient,
          { topicName: TOPIC_NAME, queueUrl, startupResourcePolling },
          undefined,
          undefined,
          { onResourcesReady },
        )

        expect(result).toBeUndefined()

        const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)

        await vi.waitFor(() => expect(onResourcesReady).toHaveBeenCalledTimes(1), {
          timeout: 2000,
          interval: 50,
        })
        expect(onResourcesReady).toHaveBeenCalledWith({
          topicArn,
          queueUrl,
          queueArn,
          queueName: QUEUE_NAME,
          subscriptionArn,
        })
      })

      it('returns resources without invoking onResourcesReady when available on first check', async () => {
        const topicArn = await testAdmin.createTopic(TOPIC_NAME)
        const { queueArn } = await testAdmin.createQueue(QUEUE_NAME)
        const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)
        const onResourcesReady = vi.fn()

        const result = await initSnsSqs(
          sqsClient,
          snsClient,
          stsClient,
          { topicName: TOPIC_NAME, queueUrl, startupResourcePolling },
          undefined,
          undefined,
          { onResourcesReady },
        )

        expect(result).toEqual({
          topicArn,
          queueUrl,
          queueArn,
          queueName: QUEUE_NAME,
          subscriptionArn,
        })
        // Give time to a potential background resolution
        await setTimeout(200)
        expect(onResourcesReady).not.toHaveBeenCalled()
      })

      it('throws errors other than missing resources', async () => {
        await testAdmin.createTopic(TOPIC_NAME)
        await testAdmin.createQueue(QUEUE_NAME)
        const onResourcesReady = vi.fn()
        const originalSend = snsClient.send.bind(snsClient)
        vi.spyOn(snsClient, 'send').mockImplementation((command) => {
          if (command instanceof ListSubscriptionsByTopicCommand) {
            return Promise.reject(new Error('Access denied'))
          }
          return originalSend(command)
        })

        await expect(
          initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            { topicName: TOPIC_NAME, queueUrl, startupResourcePolling },
            undefined,
            undefined,
            { onResourcesReady },
          ),
        ).rejects.toThrow('Access denied')
        expect(onResourcesReady).not.toHaveBeenCalled()
      })
    })
  })
})
