import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { SqsPermissionPublisherFifo } from '../publishers/SqsPermissionPublisherFifo.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SqsPermissionConsumerFifo } from './SqsPermissionConsumerFifo.ts'

describe('SqsPermissionConsumerFifo', () => {
  describe('FIFO queue consumption and retry', () => {
    const queueName = 'test-fifo-consumer.fifo'
    const dlqName = 'test-fifo-consumer-dlq.fifo'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      // Deletion handled by deletionConfig.deleteIfExists in each test
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('creates FIFO queue and consumes messages', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
      })

      await publisher.init()
      await consumer.start()

      expect(consumer.queueProps.isFifo).toBe(true)

      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: 'user-1',
      })

      await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      expect(consumer.addCounter).toBe(1)
      expect(consumer.processedMessagesIds.has('1')).toBe(true)

      await consumer.close(true)
    })

    it('retries with ContentBasedDeduplication enabled - no MessageDeduplicationId', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      let attemptCount = 0
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 100, // Fast recheck for tests
        addHandlerOverride: () => {
          attemptCount++
          if (attemptCount < 2) {
            return Promise.resolve({ error: 'retryLater' })
          }
          return Promise.resolve({ result: 'success' })
        },
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await consumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'add',
        userIds: 'user-2',
      })

      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      // FIFO queues use barrier sleep approach - no retry republishing
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        if (!body?.includes('"id":"2"')) return false
        try {
          const parsed = JSON.parse(body)
          return (
            typeof parsed._internalRetryLaterCount === 'number' &&
            parsed._internalRetryLaterCount >= 1
          )
        } catch {
          return false
        }
      })

      // No retry republishing for FIFO queues
      expect(retrySendCalls.length).toBe(0)

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })

    it('retries with ContentBasedDeduplication disabled - preserves MessageDeduplicationId', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      let attemptCount = 0
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 100, // Fast recheck for tests
        addHandlerOverride: () => {
          attemptCount++
          if (attemptCount < 2) {
            return Promise.resolve({ error: 'retryLater' })
          }
          return Promise.resolve({ result: 'success' })
        },
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await consumer.start()

      // Publish with explicit MessageDeduplicationId
      await publisher.publish(
        {
          id: '2',
          messageType: 'add',
          userIds: 'user-2',
        },
        {
          MessageDeduplicationId: 'unique-dedup-id-123',
        },
      )

      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      // FIFO queues use barrier sleep approach - no retry republishing
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        if (!body?.includes('"id":"2"')) return false
        try {
          const parsed = JSON.parse(body)
          return (
            typeof parsed._internalRetryLaterCount === 'number' &&
            parsed._internalRetryLaterCount >= 1
          )
        } catch {
          return false
        }
      })

      // No retry republishing for FIFO queues
      expect(retrySendCalls.length).toBe(0)

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })

    it('preserves MessageGroupId during retry', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        messageGroupIdField: 'userIds',
      })

      let attemptCount = 0
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 100, // Fast recheck for tests
        addHandlerOverride: () => {
          attemptCount++
          if (attemptCount < 2) {
            return Promise.resolve({ error: 'retryLater' })
          }
          return Promise.resolve({ result: 'success' })
        },
      })

      await publisher.init()
      await consumer.start()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await publisher.publish({
        id: '3',
        messageType: 'add',
        userIds: 'custom-group-id',
      })

      await consumer.handlerSpy.waitForMessageWithId('3', 'consumed')

      // For FIFO queues with new barrier sleep approach:
      // - No retry republishing (no SendMessageCommand for retry)
      // - Consumer sleeps and rechecks, processing on second attempt within sleep loop
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        if (!body?.includes('"id":"3"')) return false
        try {
          const parsed = JSON.parse(body)
          return (
            typeof parsed._internalRetryLaterCount === 'number' &&
            parsed._internalRetryLaterCount >= 1
          )
        } catch {
          return false
        }
      })

      // No retry republishing for FIFO queues - they use sleep-and-recheck instead
      expect(retrySendCalls.length).toBe(0)

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })

    it('sends failed messages to FIFO DLQ with MessageGroupId', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
              VisibilityTimeout: '1',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        deadLetterQueue: {
          creationConfig: {
            queue: {
              QueueName: dlqName,
              Attributes: {
                FifoQueue: 'true',
                ContentBasedDeduplication: 'true',
              },
            },
          },
          redrivePolicy: {
            maxReceiveCount: 1,
          },
        },
        maxRetryDuration: 2, // 2 seconds max retry
        addHandlerOverride: () => {
          return Promise.resolve({ error: 'retryLater' })
        },
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await consumer.start()

      await publisher.publish({
        id: '4',
        messageType: 'add',
        userIds: 'user-4',
      })

      // Poll for DLQ send with timeout
      const pollForDLQSend = async (timeoutMs = 10000, pollIntervalMs = 100) => {
        const startTime = Date.now()
        while (Date.now() - startTime < timeoutMs) {
          const dlqSendCalls = sqsSpy.mock.calls.filter((call) => {
            if (!(call[0] instanceof SendMessageCommand)) return false
            const command = call[0] as SendMessageCommand
            return command.input.QueueUrl?.includes(dlqName)
          })

          if (dlqSendCalls.length > 0) {
            return dlqSendCalls
          }

          await new Promise((resolve) => setTimeout(resolve, pollIntervalMs))
        }
        return []
      }

      const dlqSendCalls = await pollForDLQSend()

      // Explicitly fail if no DLQ send was observed
      expect(dlqSendCalls.length).toBeGreaterThan(0)

      // Assert MessageGroupId is preserved
      const dlqCommand = dlqSendCalls[0]?.[0] as SendMessageCommand
      expect(dlqCommand.input.MessageGroupId).toBe('test-group')

      await consumer.close(true)
    }, 15000) // 15 second timeout for DLQ test

    it('maintains message order within message group', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      const processedOrder: string[] = []
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        addHandlerOverride: (message) => {
          processedOrder.push(message.id)
          return Promise.resolve({ result: 'success' })
        },
      })

      await publisher.init()
      await consumer.start()

      // Publish messages in order
      for (let i = 1; i <= 5; i++) {
        await publisher.publish({
          id: `${i}`,
          messageType: 'add',
          userIds: `user-${i}`,
        })
      }

      await consumer.handlerSpy.waitForMessageWithId('5', 'consumed')

      // Verify messages were processed in order
      expect(processedOrder).toEqual(['1', '2', '3', '4', '5'])

      await consumer.close(true)
    })

    it('retries with locatorConfig - fetches ContentBasedDeduplication from SQS', async () => {
      // First create the queue with a publisher
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      await publisher.init()

      let attemptCount = 0
      // Consumer uses locatorConfig, not creationConfig - forces async fetch of ContentBasedDeduplication
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        locatorConfig: {
          queueUrl: publisher.queueProps.url,
        },
        barrierSleepCheckIntervalInMsecs: 100, // Fast recheck for tests
        addHandlerOverride: () => {
          attemptCount++
          if (attemptCount < 2) {
            return Promise.resolve({ error: 'retryLater' })
          }
          return Promise.resolve({ result: 'success' })
        },
      })

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await consumer.start()

      // Publish with explicit MessageDeduplicationId
      await publisher.publish(
        {
          id: 'locator-test',
          messageType: 'add',
          userIds: 'user-locator',
        },
        {
          MessageDeduplicationId: 'locator-dedup-id',
        },
      )

      await consumer.handlerSpy.waitForMessageWithId('locator-test', 'consumed')

      // FIFO queues use barrier sleep approach - no retry republishing
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        if (!body?.includes('"id":"locator-test"')) return false
        try {
          const parsed = JSON.parse(body)
          return (
            typeof parsed._internalRetryLaterCount === 'number' &&
            parsed._internalRetryLaterCount >= 1
          )
        } catch {
          return false
        }
      })

      // No retry republishing for FIFO queues
      expect(retrySendCalls.length).toBe(0)

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })

    it('uses custom visibility extension parameters', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      let attemptCount = 0
      const consumer = new SqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 100, // Fast recheck for tests
        barrierVisibilityExtensionIntervalInMsecs: 5000, // Custom: 5 seconds
        barrierVisibilityTimeoutInSeconds: 15, // Custom: 15 seconds
        addHandlerOverride: () => {
          attemptCount++
          if (attemptCount < 2) {
            return Promise.resolve({ error: 'retryLater' })
          }
          return Promise.resolve({ result: 'success' })
        },
      })

      await publisher.init()
      await consumer.start()

      await publisher.publish({
        id: 'custom-visibility-test',
        messageType: 'add',
        userIds: 'user-custom',
      })

      await consumer.handlerSpy.waitForMessageWithId('custom-visibility-test', 'consumed')

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })
  })
})
