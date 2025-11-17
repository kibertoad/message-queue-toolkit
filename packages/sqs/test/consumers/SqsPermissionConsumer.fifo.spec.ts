import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { deleteQueue } from '../../lib/utils/sqsUtils.ts'
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
      await deleteQueue(sqsClient, queueName)
      await deleteQueue(sqsClient, dlqName)
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

      await consumer.close()
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

      // Check that retry was sent without DelaySeconds
      // Filter for actual retries (_internalRetryLaterCount > 0)
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        return body?.includes('"id":"2"') && /"_internalRetryLaterCount":[1-9]/.test(body)
      })

      expect(retrySendCalls.length).toBeGreaterThan(0)

      const retryCommand = retrySendCalls[0]?.[0] as SendMessageCommand
      // FIFO queues should not have DelaySeconds
      expect(retryCommand.input.DelaySeconds).toBeUndefined()
      // FIFO queues should preserve MessageGroupId
      expect(retryCommand.input.MessageGroupId).toBe('test-group')
      // When ContentBasedDeduplication is enabled, MessageDeduplicationId should NOT be set
      // (SQS generates it based on message body)
      expect(retryCommand.input.MessageDeduplicationId).toBeUndefined()

      expect(attemptCount).toBe(2)

      await consumer.close()
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

      // Check that retry modified MessageDeduplicationId
      // Filter for actual retries (_internalRetryLaterCount > 0, not just present)
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        // Match retry attempts: _internalRetryLaterCount":1 or higher (not :0 which is initial)
        return body?.includes('"id":"2"') && /"_internalRetryLaterCount":[1-9]/.test(body)
      })

      expect(retrySendCalls.length).toBeGreaterThan(0)

      const retryCommand = retrySendCalls[0]?.[0] as SendMessageCommand
      // When ContentBasedDeduplication is disabled, MessageDeduplicationId should be modified with retry count
      // to prevent SQS from treating it as duplicate within 5-minute deduplication window
      expect(retryCommand.input.MessageDeduplicationId).toBe('unique-dedup-id-123-retry-1')
      // FIFO queues should preserve MessageGroupId
      expect(retryCommand.input.MessageGroupId).toBe('test-group')
      // FIFO queues should not have DelaySeconds
      expect(retryCommand.input.DelaySeconds).toBeUndefined()

      expect(attemptCount).toBe(2)

      await consumer.close()
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

      // Check that MessageGroupId was preserved
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        return body?.includes('"id":"3"') && body?.includes('_internalRetryLaterCount')
      })

      expect(retrySendCalls.length).toBeGreaterThan(0)

      const retryCommand = retrySendCalls[0]?.[0] as SendMessageCommand
      expect(retryCommand.input.MessageGroupId).toBe('custom-group-id')

      await consumer.close()
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

      await consumer.close()
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

      await consumer.close()
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

      // Check that retry modified MessageDeduplicationId
      const retrySendCalls = sqsSpy.mock.calls.filter((call) => {
        if (!(call[0] instanceof SendMessageCommand)) return false
        const command = call[0] as SendMessageCommand
        const body = command.input.MessageBody
        return (
          body?.includes('"id":"locator-test"') && /"_internalRetryLaterCount":[1-9]/.test(body)
        )
      })

      expect(retrySendCalls.length).toBeGreaterThan(0)

      const retryCommand = retrySendCalls[0]?.[0] as SendMessageCommand
      // Should have fetched ContentBasedDeduplication=false from SQS and modified the ID
      expect(retryCommand.input.MessageDeduplicationId).toBe('locator-dedup-id-retry-1')
      expect(retryCommand.input.MessageGroupId).toBe('test-group')

      expect(attemptCount).toBe(2)

      await consumer.close()
    })
  })
})
