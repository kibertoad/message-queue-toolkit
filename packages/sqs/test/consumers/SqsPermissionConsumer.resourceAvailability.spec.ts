import { setTimeout } from 'node:timers/promises'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { ResourceAvailabilityTimeoutError } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  type PERMISSIONS_ADD_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'

// Simple consumer for testing resource availability
class TestResourceAvailabilityConsumer extends AbstractSqsConsumer<
  PERMISSIONS_ADD_MESSAGE_TYPE,
  undefined,
  undefined
> {
  constructor(
    dependencies: SQSConsumerDependencies,
    options: {
      locatorConfig?: {
        queueUrl?: string
        queueName?: string
      }
      creationConfig?: {
        queue?: { QueueName: string }
      }
      resourceAvailabilityConfig?: {
        enabled: boolean
        timeoutMs?: number
        pollingIntervalMs?: number
      }
    },
  ) {
    super(
      dependencies,
      {
        handlers: [
          {
            schema: PERMISSIONS_ADD_MESSAGE_SCHEMA,
            handler: () => Promise.resolve({ result: 'success' }),
          },
        ],
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

describe('SqsPermissionConsumer - resourceAvailabilityConfig', () => {
  const queueName = 'resource-availability-test-queue'
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

  describe('when resourceAvailabilityConfig is enabled', () => {
    it('waits for queue to become available and initializes successfully', async () => {
      const consumer = new TestResourceAvailabilityConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
        resourceAvailabilityConfig: {
          enabled: true,
          pollingIntervalMs: 100,
          timeoutMs: 5000,
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

    it('throws ResourceAvailabilityTimeoutError when timeout is reached', async () => {
      const consumer = new TestResourceAvailabilityConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
        resourceAvailabilityConfig: {
          enabled: true,
          pollingIntervalMs: 50,
          timeoutMs: 200, // Short timeout
        },
      })

      // Should throw timeout error since queue never appears
      await expect(consumer.init()).rejects.toThrow(ResourceAvailabilityTimeoutError)
    })

    it('polls indefinitely when timeoutMs is not set', async () => {
      const consumer = new TestResourceAvailabilityConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
        resourceAvailabilityConfig: {
          enabled: true,
          pollingIntervalMs: 50,
          // No timeout - polls indefinitely
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

  describe('when resourceAvailabilityConfig is disabled', () => {
    it('throws immediately when queue does not exist', async () => {
      const consumer = new TestResourceAvailabilityConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
        resourceAvailabilityConfig: {
          enabled: false,
        },
      })

      // Should throw immediately
      await expect(consumer.init()).rejects.toThrow(/does not exist/)
    })
  })

  describe('when resourceAvailabilityConfig is not provided', () => {
    it('throws immediately when queue does not exist (default behavior)', async () => {
      const consumer = new TestResourceAvailabilityConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
      })

      // Should throw immediately (backwards compatible behavior)
      await expect(consumer.init()).rejects.toThrow(/does not exist/)
    })
  })
})
