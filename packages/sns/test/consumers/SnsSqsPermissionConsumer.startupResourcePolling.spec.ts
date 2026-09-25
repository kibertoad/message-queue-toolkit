import { setTimeout } from 'node:timers/promises'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import {
  MessageHandlerConfigBuilder,
  NO_TIMEOUT,
  StartupResourcePollingTimeoutError,
} from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  AbstractSnsSqsConsumer,
  type SNSSQSConsumerDependencies,
  type SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { initSnsSqs } from '../../lib/utils/snsInitter.ts'
import { findSubscriptionByTopicAndQueue } from '../../lib/utils/snsUtils.ts'
import { getPort } from '../utils/fauxqsInstance.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  type PERMISSIONS_ADD_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

type TestConsumerOptions = Pick<
  SNSSQSConsumerOptions<PERMISSIONS_ADD_MESSAGE_TYPE, undefined, undefined>,
  'locatorConfig' | 'creationConfig' | 'subscriptionConfig'
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
    if (!this.areResourcesReady) {
      return {
        topicArn: undefined,
        queueUrl: undefined,
        queueName: undefined,
        subscriptionArn: undefined,
      }
    }
    return {
      topicArn: this.subscription.topicArn,
      queueUrl: this.queue.url,
      queueName: this.queue.name,
      subscriptionArn: this.subscription.subscriptionArn,
    }
  }
}

describe('SnsSqsPermissionConsumer - startupResourcePollingConfig', () => {
  const queueName = 'startup-resource-polling-test-queue'
  const topicName = 'startup-resource-polling-test-topic'
  const queueUrl = `http://sqs.eu-west-1.localstack:${getPort()}/000000000000/${queueName}`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let snsClient: SNSClient
  let stsClient: STSClient
  let testAdmin: TestAwsResourceAdmin

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    sqsClient = diContainer.cradle.sqsClient
    snsClient = diContainer.cradle.snsClient
    stsClient = diContainer.cradle.stsClient
    testAdmin = diContainer.cradle.testAdmin
  })

  beforeEach(async () => {
    await testAdmin.deleteQueues(queueName)
    await testAdmin.deleteTopics(topicName)
  })

  afterEach(async () => {
    await testAdmin.deleteQueues(queueName)
    await testAdmin.deleteTopics(topicName)
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('when startupResourcePolling is enabled', () => {
    it('waits for topic to become available and initializes successfully', async () => {
      // Create queue first, but not the topic
      await testAdmin.createQueue(queueName)

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
      const topicArn = await testAdmin.createTopic(topicName)

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })

    it('waits for queue to become available and initializes successfully', async () => {
      // Create topic first, but not the queue
      const topicArn = await testAdmin.createTopic(topicName)

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
      await testAdmin.createQueue(queueName)

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
    })

    it('throws StartupResourcePollingTimeoutError when timeout is reached', async () => {
      // Create queue but not topic
      await testAdmin.createQueue(queueName)

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
      await testAdmin.createQueue(queueName)

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
      const topicArn = await testAdmin.createTopic(topicName)

      // Init should complete successfully
      await initPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })

    it('start() waits for resources and starts consumers (blocking mode)', async () => {
      // Create queue first, but not the topic
      await testAdmin.createQueue(queueName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:dummy:bdf640a2-bedf-475a-98b8-758b88c87395',
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: 5000,
          },
        },
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Consumer should not be running before start
      expect(consumer.isRunning).toBe(false)

      // Start in background (blocking mode waits for resources)
      const startPromise = consumer.start()

      // Wait a bit then create the topic
      await setTimeout(200)
      const topicArn = await testAdmin.createTopic(topicName)

      // start() should complete successfully after topic appears
      await startPromise

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
      // Consumer should be running after start completes
      expect(consumer.isRunning).toBe(true)

      // Clean up
      await consumer.close()
    })

    it('start() works immediately when resources already exist (blocking mode)', async () => {
      // Create both resources before starting
      await testAdmin.createQueue(queueName)
      const topicArn = await testAdmin.createTopic(topicName)

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

      // Consumer should not be running before start
      expect(consumer.isRunning).toBe(false)

      // start() should complete immediately since resources exist
      await consumer.start()

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
      // Consumer should be running after start completes
      expect(consumer.isRunning).toBe(true)

      // Clean up
      await consumer.close()
    })
  })

  describe('when nonBlocking mode is enabled', () => {
    it('returns immediately when both resources are available on first check', async () => {
      // Create queue first, then topic (order matters for LocalStack)
      await testAdmin.createQueue(queueName)
      const topicArn = await testAdmin.createTopic(topicName)

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
      await testAdmin.createQueue(queueName)

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

      // Init should return immediately even though topic doesn't exist;
      // resources are populated together via the background callback, so
      // none of subscriptionProps' fields are set yet.
      await consumer.init()

      expect(consumer.subscriptionProps.queueName).toBeUndefined()
      expect(consumer.subscriptionProps.queueUrl).toBeUndefined()
      expect(consumer.subscriptionProps.topicArn).toBeUndefined()
    })

    it('returns immediately when queue is not available', async () => {
      // Create topic but not queue
      const topicArn = await testAdmin.createTopic(topicName)

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

      // Init should return immediately even though queue doesn't exist;
      // resources are populated together, so subscriptionProps stays empty
      // until the background callback fires.
      await consumer.init()

      expect(consumer.subscriptionProps.topicArn).toBeUndefined()
      expect(consumer.subscriptionProps.queueUrl).toBeUndefined()
    })

    it('start() returns immediately when topic is not available and starts consumers when resources become ready', async () => {
      // Create queue but not topic
      await testAdmin.createQueue(queueName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
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
        creationConfig: {
          queue: { QueueName: queueName },
        },
      })

      // Consumer should not be running before start
      expect(consumer.isRunning).toBe(false)

      // start() should return immediately even though topic doesn't exist
      await consumer.start()

      // Resources are populated together by the background callback once ready,
      // so subscriptionProps stays empty here and isRunning is still false.
      expect(consumer.subscriptionProps.queueName).toBeUndefined()
      expect(consumer.isRunning).toBe(false)

      // Create topic after start returns
      const topicArn = await testAdmin.createTopic(topicName)

      // Wait for consumer to start running (happens when resources become ready)
      await vi.waitFor(
        () => {
          expect(consumer.isRunning).toBe(true)
        },
        { timeout: 3000, interval: 50 },
      )

      // Verify topicArn was updated
      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)

      // Clean up
      await consumer.close()
    })

    it('start() works immediately when resources are already available', async () => {
      // Create both queue and topic before starting
      await testAdmin.createQueue(queueName)
      const topicArn = await testAdmin.createTopic(topicName)

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

      // Consumer should not be running before start
      expect(consumer.isRunning).toBe(false)

      // start() should work immediately since resources exist
      await consumer.start()

      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
      // Consumer should be running after start completes (resources were available)
      expect(consumer.isRunning).toBe(true)

      // Clean up
      await consumer.close()
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
      const topicArn = await testAdmin.createTopic(topicName)

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

  })

  describe('when subscription is located (subscriptionConfig is not provided)', () => {
    it('waits for subscription to become available and initializes successfully', async () => {
      const topicArn = await testAdmin.createTopic(topicName)
      const { queueArn } = await testAdmin.createQueue(queueName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          startupResourcePolling: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000 },
        },
        subscriptionConfig: undefined,
      })

      const initPromise = consumer.init()

      // Wait a bit then subscribe the queue, as infrastructure tooling would do
      await setTimeout(300)
      const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)

      await initPromise

      expect(consumer.subscriptionProps.subscriptionArn).toBe(subscriptionArn)
      expect(consumer.subscriptionProps.topicArn).toBe(topicArn)
      expect(consumer.subscriptionProps.queueUrl).toBe(queueUrl)
    })

    it('throws StartupResourcePollingTimeoutError when subscription never appears', async () => {
      await testAdmin.createTopic(topicName)
      await testAdmin.createQueue(queueName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueUrl,
          startupResourcePolling: { enabled: true, pollingIntervalMs: 50, timeoutMs: 200 },
        },
        subscriptionConfig: undefined,
      })

      await expect(consumer.init()).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('waits for queue located by name to become available', async () => {
      const topicArn = await testAdmin.createTopic(topicName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueName,
          startupResourcePolling: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000 },
        },
        subscriptionConfig: undefined,
      })

      const initPromise = consumer.init()

      await setTimeout(300)
      const { queueArn } = await testAdmin.createQueue(queueName)
      const subscriptionArn = await testAdmin.createSubscription(topicArn, queueArn)

      await initPromise

      expect(consumer.subscriptionProps.subscriptionArn).toBe(subscriptionArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })
  })

  describe('when topic and queue are located and subscription is created', () => {
    it('waits for queue to become available before creating subscription', async () => {
      const topicArn = await testAdmin.createTopic(topicName)

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          topicName,
          queueName,
          startupResourcePolling: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000 },
        },
      })

      const initPromise = consumer.init()

      await setTimeout(300)
      const { queueArn } = await testAdmin.createQueue(queueName)

      await initPromise

      const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
      expect(subscription).toBeDefined()
      expect(consumer.subscriptionProps.subscriptionArn).toBe(subscription?.SubscriptionArn)
      expect(consumer.subscriptionProps.queueName).toBe(queueName)
    })
  })

  describe('when startupResourcePolling is disabled', () => {
    it('throws immediately when topic does not exist', async () => {
      // Create queue but not topic
      await testAdmin.createQueue(queueName)

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
      const topicArn = await testAdmin.createTopic(topicName)

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
