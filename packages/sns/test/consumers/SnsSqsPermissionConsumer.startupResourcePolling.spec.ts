import { setTimeout } from 'node:timers/promises'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import {
  MessageHandlerConfigBuilder,
  StartupResourcePollingTimeoutError,
} from '@message-queue-toolkit/core'
import { assertQueue, deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import {
  AbstractSnsSqsConsumer,
  type SNSSQSConsumerDependencies,
  type SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer.ts'
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
