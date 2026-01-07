import { setTimeout } from 'node:timers/promises'
import type { SQSClient } from '@aws-sdk/client-sqs'
import {
  MessageHandlerConfigBuilder,
  NO_TIMEOUT,
  StartupResourcePollingTimeoutError,
} from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  AbstractSqsConsumer,
  type SQSConsumerDependencies,
  type SQSConsumerOptions,
} from '../../lib/sqs/AbstractSqsConsumer.ts'
import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  type PERMISSIONS_ADD_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

type TestConsumerOptions = Pick<
  SQSConsumerOptions<PERMISSIONS_ADD_MESSAGE_TYPE, undefined, undefined>,
  'locatorConfig' | 'creationConfig'
>

// Simple consumer for testing startup resource polling
class TestStartupResourcePollingConsumer extends AbstractSqsConsumer<
  PERMISSIONS_ADD_MESSAGE_TYPE,
  undefined,
  undefined
> {
  constructor(dependencies: SQSConsumerDependencies, options: TestConsumerOptions) {
    super(
      dependencies,
      {
        handlers: new MessageHandlerConfigBuilder<PERMISSIONS_ADD_MESSAGE_TYPE, undefined>()
          .addConfig(PERMISSIONS_ADD_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' }))
          .build(),
        messageTypeResolver: { messageTypePath: 'messageType' },
        ...options,
      },
      undefined,
    )
  }

  get queueProps() {
    return {
      url: this.queueUrl,
      name: this.queueName,
      arn: this.queueArn,
    }
  }
}

describe('SqsPermissionConsumer - startupResourcePollingConfig', () => {
  const queueName = 'startup-resource-polling-test-queue'
  const queueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient

  beforeEach(async () => {
    diContainer = await registerDependencies()
    sqsClient = diContainer.cradle.sqsClient
    await deleteQueue(sqsClient, queueName)
  })

  afterEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('when startupResourcePolling is enabled', () => {
    it('waits for queue to become available and initializes successfully', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
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

      expect(consumer.queueProps.url).toBe(queueUrl)
      expect(consumer.queueProps.name).toBe(queueName)
    })

    it('throws StartupResourcePollingTimeoutError when timeout is reached', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: 200, // Short timeout
          },
        },
      })

      // Should throw timeout error since queue never appears
      await expect(consumer.init()).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('polls indefinitely when NO_TIMEOUT is used', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 50,
            timeoutMs: NO_TIMEOUT,
          },
        },
      })

      // Start init in background
      const initPromise = consumer.init()

      // Wait a bit then create the queue
      await setTimeout(500)
      await assertQueue(sqsClient, { QueueName: queueName })

      // Init should complete successfully
      await initPromise

      expect(consumer.queueProps.url).toBe(queueUrl)
    })
  })

  describe('when startupResourcePolling is disabled', () => {
    it('throws immediately when queue does not exist', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
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

  describe('when startupResourcePolling is not provided', () => {
    it('throws immediately when queue does not exist (default behavior)', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
      })

      // Should throw immediately (backwards compatible behavior)
      await expect(consumer.init()).rejects.toThrow(/does not exist/)
    })
  })

  describe('when nonBlocking mode is enabled', () => {
    it('returns immediately when resource is available on first check', async () => {
      // Create queue first
      await assertQueue(sqsClient, { QueueName: queueName })

      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
            nonBlocking: true,
          },
        },
      })

      await consumer.init()

      expect(consumer.queueProps.url).toBe(queueUrl)
      expect(consumer.queueProps.arn).toBeDefined()
    })

    it('returns immediately when resource is not available and queueArn is undefined', async () => {
      const consumer = new TestStartupResourcePollingConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
          startupResourcePolling: {
            enabled: true,
            pollingIntervalMs: 100,
            timeoutMs: 5000,
            nonBlocking: true,
          },
        },
      })

      // Init should complete immediately even though queue doesn't exist
      await consumer.init()

      expect(consumer.queueProps.url).toBe(queueUrl)
      expect(consumer.queueProps.arn).toBeUndefined()
    })

    it('invokes onQueueReady callback when resource becomes available in background', async () => {
      // We need to test the callback at the initter level since AbstractSqsConsumer
      // doesn't expose the onQueueReady callback directly
      const { initSqs } = await import('../../lib/utils/sqsInitter.ts')

      let callbackInvoked = false
      let callbackArn: string | undefined

      // Start init without queue existing
      const initPromise = initSqs(
        sqsClient,
        { queueUrl, startupResourcePolling: { enabled: true, pollingIntervalMs: 50, timeoutMs: 5000, nonBlocking: true } },
        undefined,
        undefined,
        {
          onQueueReady: (result) => {
            callbackInvoked = true
            callbackArn = result.queueArn
          },
        },
      )

      // Init should return immediately
      const result = await initPromise
      expect(result.queueArn).toBeUndefined()

      // Create queue after init returns
      await assertQueue(sqsClient, { QueueName: queueName })

      // Wait for background polling to detect the queue
      await setTimeout(200)

      expect(callbackInvoked).toBe(true)
      expect(callbackArn).toBeDefined()
    })
  })
})
