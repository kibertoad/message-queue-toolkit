import { setTimeout } from 'node:timers/promises'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import {
  MessageHandlerConfigBuilder,
  NO_TIMEOUT,
  StartupResourcePollingTimeoutError,
} from '@message-queue-toolkit/core'
import { assertQueue, deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  AbstractSnsSqsConsumer,
  type SNSSQSConsumerDependencies,
  type SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { initSnsSqs } from '../../lib/utils/snsInitter.ts'
import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  type PERMISSIONS_ADD_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

type TestConsumerOptions = Pick<
  SNSSQSConsumerOptions<PERMISSIONS_ADD_MESSAGE_TYPE, undefined, undefined>,
  'locatorConfig' | 'creationConfig'
>

// Simple consumer for testing startup resource polling
class TestStartupResourcePollingConsumer extends AbstractSnsSqsConsumer<
  PERMISSIONS_ADD_MESSAGE_TYPE,
  undefined,
  undefined
> {
  constructor(dependencies: SNSSQSConsumerDependencies, options: TestConsumerOptions) {
    super(
      dependencies,
      {
        handlers: new MessageHandlerConfigBuilder<PERMISSIONS_ADD_MESSAGE_TYPE, undefined>()
          .addConfig(PERMISSIONS_ADD_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' }))
          .build(),
        messageTypeResolver: { messageTypePath: 'messageType' },
        subscriptionConfig: { updateAttributesIfExists: false },
        ...options,
      },
      undefined,
    )
  }

  get subscriptionProps() {
    return {
      topicArn: this.topicArn,
      queueUrl: this.queueUrl,
      queueName: this.queueName,
      subscriptionArn: this.subscriptionArn,
    }
  }
}

describe('SnsSqsPermissionConsumer - startupResourcePollingConfig', () => {
  const queueName = 'startup-resource-polling-test-queue'
  const topicName = 'startup-resource-polling-test-topic'
  const queueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await deleteTopic(snsClient, stsClient, topicName)
  })

  afterEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await deleteTopic(snsClient, stsClient, topicName)
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('when startupResourcePolling is enabled', () => {
    it('waits for topic to become available and initializes successfully', async () => {
      // Create queue first, but not the topic
      await assertQueue(sqsClient, { QueueName: queueName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Start init in background
      const initPromise = consumer.init()

      // Wait a bit then create the topic
      await setTimeout(300)
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })

    it('waits for queue to become available and initializes successfully', async () => {
      // Create topic first, but not the queue
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
          },
        },
      })

      // Start init in background
      const initPromise = consumer.init()

      // Wait a bit then create the queue
      await setTimeout(300)
      await assertQueue(sqsClient, { QueueName: queueName })

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
    })

    it('throws StartupResourcePollingTimeoutError when timeout is reached', async () => {
      // Create queue but not topic
      await assertQueue(sqsClient, { QueueName: queueName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: 200, // Short timeout
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Should throw timeout error since topic never appears
      await expect(consumer.init()).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('polls indefinitely when NO_TIMEOUT is used', async () => {
      // Create queue first, but not the topic
      await assertQueue(sqsClient, { QueueName: queueName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: NO_TIMEOUT,
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Start init in background
      const initPromise = consumer.init()

      // Wait a bit then create the topic
      await setTimeout(500)
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })
  })

  describe('when nonBlocking mode is enabled', () => {
    it('returns immediately when both resources are available on first check', async () => {
      // Create queue first, then topic (order matters for LocalStack)
      await assertQueue(sqsClient, { QueueName: queueName })
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
            nonBlocking: true,
          },
        },
      })

      await consumer.init()

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
    })

    it('returns immediately when topic is not available', async () => {
      // Create queue but not topic
      await assertQueue(sqsClient, { QueueName: queueName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
            nonBlocking: true,
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Init should return immediately even though topic doesn't exist
      await consumer.init()

      // queueName should still be set
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })

    it('returns immediately when queue is not available', async () => {
      // Create topic but not queue
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
            nonBlocking: true,
          },
        },
      })

      // Init should return immediately even though queue doesn't exist
      await consumer.init()

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
    })

    it('invokes onResourcesReady callback when both resources become available in background', async () => {
      // Test at the initter level since AbstractSnsSqsConsumer doesn't expose the callback

      let callbackInvoked = false
      let callbackTopicArn: string | undefined
      let callbackQueueUrl: string | undefined

      // Create queue but not topic
      await assertQueue(sqsClient, { QueueName: queueName })

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

      expect(result.resourcesReady).toBe(false)

      // Create topic after init returns
      await assertTopic(snsClient, stsClient, { Name: topicName })

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

      // Should return immediately with resourcesReady: false
      expect(result.resourcesReady).toBe(false)

      // Create topic so topic polling succeeds, but NOT queue
      await assertTopic(snsClient, stsClient, { Name: topicName })

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
  })

  describe('when subscriptionArn is not provided (subscription creation mode)', () => {
    it('waits for topic to become available before creating subscription', async () => {
      // No topic and no queue exist initially

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          // No subscriptionArn - will create subscription
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Start init in background
      const initPromise = consumer.init()

      // Wait a bit then create the topic (queue will be created by assertQueue in subscribeToTopic)
      await setTimeout(300)
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      // Init should complete successfully after topic is created
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
      expect(consumer.subscriptionProps.subscriptionArn).toBeDefined()
    })

    it('throws StartupResourcePollingTimeoutError when topic never appears', async () => {
      // No topic exists

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          // No subscriptionArn - will create subscription
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: 200, // Short timeout
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Should throw timeout error since topic never appears
      await expect(consumer.init()).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('returns immediately in non-blocking mode when topic not available', async () => {
      // Test at the initter level since consumer.init() sets internal state

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

      // Should return immediately with resourcesReady: false
      expect(result.resourcesReady).toBe(false)
      expect(result.subscriptionArn).toBe('')
      expect(result.queueName).toBe(queueName)
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

      // Should return immediately with resourcesReady: false
      expect(result.resourcesReady).toBe(false)
      expect(result.subscriptionArn).toBe('')

      // Create topic after init returns
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

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
  })

  describe('when startupResourcePolling is disabled', () => {
    it('throws immediately when topic does not exist', async () => {
      // Create queue but not topic
      await assertQueue(sqsClient, { QueueName: queueName })

      const topicArn = 'arn:aws:sns:eu-west-1:000000000000:non-existent-topic'

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: false,
            timeoutMs: 5000,
          },
        },
      })

      // Should throw immediately
      await expect(consumer.init()).rejects.toThrow(/does not exist/)
    })

    it('throws immediately when queue does not exist', async () => {
      // Create topic but not queue
      const topicArn = await assertTopic(snsClient, stsClient, { Name: topicName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: false,
            timeoutMs: 5000,
          },
        },
      })

      // Should throw immediately
      await expect(consumer.init()).rejects.toThrow(/does not exist/)
    })
  })
})
