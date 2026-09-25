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

const topicName = 'sns-initter-topic'
const queueName = 'sns-initter-queue'

describe('snsInitter', () => {
  const queueUrl = `http://sqs.eu-west-1.localstack:${getPort()}/000000000000/${queueName}`

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

    await testAdmin.deleteQueues(queueName)
    await testAdmin.deleteTopics(topicName)
  })

  describe('initSnsSqs', () => {
    describe('resource resolution', () => {
      it('creates subscription when topic and queue are located', async () => {
        const topicArn = await testAdmin.createTopic(topicName)
        const { queueArn } = await testAdmin.createQueue(queueName)

        const result = await initSnsSqs(
          sqsClient,
          snsClient,
          stsClient,
          { topicName, queueName },
          undefined,
          { updateAttributesIfExists: false },
        )

        const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
        expect(subscription).toBeDefined()
        expect(result).toEqual({
          topicArn,
          queueUrl,
          queueArn,
          queueName,
          subscriptionArn: subscription?.SubscriptionArn,
        })
      })

      it('locates existing subscription when subscription config is not provided', async () => {
        const topicArn = await testAdmin.createTopic(topicName)
        const { queueArn } = await testAdmin.createQueue(queueName)
        const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)
        const snsSpy = vi.spyOn(snsClient, 'send')

        const result = await initSnsSqs(sqsClient, snsClient, stsClient, {
          topicName,
          queueName,
        })

        expect(result).toEqual({
          topicArn,
          queueUrl,
          queueArn,
          queueName,
          subscriptionArn,
        })
        // Subscription is only located, never created
        expect(snsSpy).not.toHaveBeenCalledWith(expect.any(SubscribeCommand))
      })

      it('throws when subscription to locate does not exist', async () => {
        await testAdmin.createTopic(topicName)
        await testAdmin.createQueue(queueName)

        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, {
            topicName,
            queueName,
          }),
        ).rejects.toThrow(/Subscription of queue .* to topic .* does not exist/)
      })

      it('ignores subscription pending confirmation when locating it', async () => {
        await testAdmin.createTopic(topicName)
        const { queueArn } = await testAdmin.createQueue(queueName)
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
            topicName,
            queueName,
          }),
        ).rejects.toThrow(/Subscription of queue .* to topic .* does not exist/)
      })
    })

    describe('config validation', () => {
      it('throws when neither topic creation config nor topic locator is provided', async () => {
        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, { queueName }, undefined, {
            updateAttributesIfExists: false,
          }),
        ).rejects.toThrow(
          /creationConfig.topic is mandatory .* OR locatorConfig.name or locatorConfig.topicArn parameter is mandatory/,
        )
      })

      it('throws when neither queue creation config nor queue locator is provided', async () => {
        await expect(
          initSnsSqs(sqsClient, snsClient, stsClient, { topicName }, undefined, {
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
            { topicName },
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
            { topicName },
            { queue: { QueueName: queueName } },
          ),
        ).rejects.toThrow(
          'If creationConfig.queue is specified, subscriptionConfig is mandatory, as the subscription of a queue being created cannot be located',
        )
      })
    })

    describe('non-blocking startup resource polling', () => {
      describe('when subscription ARN is provided', () => {
        it('invokes onResourcesReady callback when both resources become available in background', async () => {
          let callbackInvoked = false
          let callbackTopicArn: string | undefined
          let callbackQueueUrl: string | undefined

          // Create queue but not topic
          await testAdmin.createQueue(queueName)

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              queueUrl,
              subscriptionArn:
                'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 50,
                timeoutMs: 5000,
                nonBlocking: true,
              },
            },
            undefined,
            undefined,
            {
              onResourcesReady: ({ topicArn, queueUrl: url }) => {
                callbackInvoked = true
                callbackTopicArn = topicArn
                callbackQueueUrl = url
              },
            },
          )

          expect(result).toBeUndefined()

          // Create topic after init returns
          await testAdmin.createTopic(topicName)

          // Wait for callback to be invoked using vi.waitFor (more reliable than fixed timeout)
          await vi.waitFor(
            () => {
              expect(callbackInvoked).toBe(true)
            },
            { timeout: 2000, interval: 50 },
          )

          expect(callbackTopicArn).toBeDefined()
          expect(callbackQueueUrl).toBe(queueUrl)
        })

        it('invokes onResourcesError callback when background queue polling times out', async () => {
          // Neither topic nor queue exist initially
          // Topic will be created after init returns, queue never appears

          let errorCallbackInvoked = false
          let callbackError: Error | undefined
          let callbackContext: { isFinal: boolean } | undefined

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              queueUrl,
              subscriptionArn:
                'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 50,
                timeoutMs: 200, // Short timeout so it fails quickly
                nonBlocking: true,
              },
            },
            undefined,
            undefined,
            {
              onResourcesError: (error, context) => {
                errorCallbackInvoked = true
                callbackError = error
                callbackContext = context
              },
            },
          )

          // Should return immediately (resources not ready in non-blocking mode)
          expect(result).toBeUndefined()

          // Create topic so topic polling succeeds, but NOT queue
          await testAdmin.createTopic(topicName)

          // Wait for error callback to be invoked (queue polling timeout)
          // Need to wait for: topic to become available (so topic polling succeeds)
          // + queue polling timeout (200ms) + some buffer
          await vi.waitFor(
            () => {
              expect(errorCallbackInvoked).toBe(true)
            },
            { timeout: 3000, interval: 50 },
          )

          expect(callbackError).toBeDefined()
          expect(callbackError?.message).toContain('Timeout')
          expect(callbackContext).toEqual({ isFinal: true })
        })

        it('invokes onResourcesError callback when background topic polling times out', async () => {
          // Neither topic nor queue exist initially
          // Topic never appears - will timeout

          let errorCallbackInvoked = false
          let callbackError: Error | undefined
          let callbackContext: { isFinal: boolean } | undefined

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              queueUrl,
              subscriptionArn:
                'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 50,
                timeoutMs: 200, // Short timeout so it fails quickly
                nonBlocking: true,
              },
            },
            undefined,
            undefined,
            {
              onResourcesError: (error, context) => {
                errorCallbackInvoked = true
                callbackError = error
                callbackContext = context
              },
            },
          )

          // Should return immediately (resources not ready in non-blocking mode)
          expect(result).toBeUndefined()

          // Don't create topic - let it timeout

          // Wait for error callback to be invoked (topic polling timeout)
          await vi.waitFor(
            () => {
              expect(errorCallbackInvoked).toBe(true)
            },
            { timeout: 2000, interval: 50 },
          )

          expect(callbackError).toBeDefined()
          expect(callbackError?.message).toContain('Timeout')
          expect(callbackContext).toEqual({ isFinal: true })
        })
      })

      describe('when subscription is created', () => {
        it('returns immediately in non-blocking mode when topic not available', async () => {
          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              // No subscriptionArn
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 100,
                timeoutMs: 5000,
                nonBlocking: true,
              },
            },
            {
              queue: { QueueName: queueName },
            },
            { updateAttributesIfExists: false },
          )

          // Should return immediately (resources not ready in non-blocking mode);
          // subscription will be created in background via onResourcesReady.
          expect(result).toBeUndefined()
        })

        it('creates subscription in background when topic becomes available in non-blocking mode', async () => {
          // No topic exists initially

          let callbackInvoked = false
          let callbackTopicArn: string | undefined
          let callbackQueueUrl: string | undefined

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              // No subscriptionArn - will create subscription
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 50,
                timeoutMs: 5000,
                nonBlocking: true,
              },
            },
            {
              queue: { QueueName: queueName },
            },
            { updateAttributesIfExists: false },
            {
              onResourcesReady: ({ topicArn, queueUrl: url }) => {
                callbackInvoked = true
                callbackTopicArn = topicArn
                callbackQueueUrl = url
              },
            },
          )

          // Should return immediately (resources not ready in non-blocking mode);
          // subscription will be created in background via onResourcesReady.
          expect(result).toBeUndefined()

          // Create topic after init returns
          const topicArn = await testAdmin.createTopic(topicName)

          // Wait for callback to be invoked (subscription created in background)
          await vi.waitFor(
            () => {
              expect(callbackInvoked).toBe(true)
            },
            { timeout: 3000, interval: 50 },
          )

          expect(callbackTopicArn).toBe(topicArn)
          expect(callbackQueueUrl).toContain(queueName)
        })

        it('creates subscription immediately when topic is available in non-blocking mode', async () => {
          // Topic exists before init
          const topicArn = await testAdmin.createTopic(topicName)

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              // No subscriptionArn - will create subscription
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 100,
                timeoutMs: 5000,
                nonBlocking: true,
              },
            },
            {
              queue: { QueueName: queueName },
            },
            { updateAttributesIfExists: false },
          )

          // Topic was immediately available, so initSnsSqs resolved synchronously
          // with the full result handle.
          expect(result).toBeDefined()
          expect(result?.subscriptionArn).toBeDefined()
          expect(result?.subscriptionArn).not.toBe('')
          expect(result?.topicArn).toBe(topicArn)
          expect(result?.queueName).toBe(queueName)
        })

        it('invokes onResourcesError callback when topic polling times out in non-blocking mode', async () => {
          // No topic exists - will timeout

          let errorCallbackInvoked = false
          let callbackError: Error | undefined
          let callbackContext: { isFinal: boolean } | undefined

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            {
              topicName,
              // No subscriptionArn - will create subscription
              startupResourcePolling: {
                enabled: true,
                pollingIntervalMs: 50,
                timeoutMs: 200, // Short timeout so it fails quickly
                nonBlocking: true,
              },
            },
            {
              queue: { QueueName: queueName },
            },
            { updateAttributesIfExists: false },
            {
              onResourcesError: (error, context) => {
                errorCallbackInvoked = true
                callbackError = error
                callbackContext = context
              },
            },
          )

          // Should return immediately (resources not ready in non-blocking mode)
          expect(result).toBeUndefined()

          // Wait for error callback to be invoked (topic polling timeout)
          await vi.waitFor(
            () => {
              expect(errorCallbackInvoked).toBe(true)
            },
            { timeout: 2000, interval: 50 },
          )

          expect(callbackError).toBeDefined()
          expect(callbackError?.message).toContain('Timeout')
          expect(callbackContext).toEqual({ isFinal: true })
        })
      })

      describe('when subscription is located', () => {
        const startupResourcePolling = {
          enabled: true,
          pollingIntervalMs: 50,
          timeoutMs: 5000,
          nonBlocking: true,
        }

        it('invokes onResourcesReady callback when located subscription becomes available', async () => {
          const topicArn = await testAdmin.createTopic(topicName)
          const { queueArn } = await testAdmin.createQueue(queueName)
          const onResourcesReady = vi.fn()

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            { topicName, queueUrl, startupResourcePolling },
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
            queueName,
            subscriptionArn,
          })
        })

        it('returns resources without invoking onResourcesReady when available on first check', async () => {
          const topicArn = await testAdmin.createTopic(topicName)
          const { queueArn } = await testAdmin.createQueue(queueName)
          const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)
          const onResourcesReady = vi.fn()

          const result = await initSnsSqs(
            sqsClient,
            snsClient,
            stsClient,
            { topicName, queueUrl, startupResourcePolling },
            undefined,
            undefined,
            { onResourcesReady },
          )

          expect(result).toEqual({
            topicArn,
            queueUrl,
            queueArn,
            queueName,
            subscriptionArn,
          })
          // Give time to a potential background resolution
          await setTimeout(200)
          expect(onResourcesReady).not.toHaveBeenCalled()
        })

        it('throws errors other than missing resources', async () => {
          await testAdmin.createTopic(topicName)
          await testAdmin.createQueue(queueName)
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
              { topicName, queueUrl, startupResourcePolling },
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
})
