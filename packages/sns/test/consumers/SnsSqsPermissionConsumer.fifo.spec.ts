import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SnsPermissionPublisherFifo } from '../publishers/SnsPermissionPublisherFifo.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsSqsPermissionConsumerFifo } from './SnsSqsPermissionConsumerFifo.ts'

describe('SnsSqsPermissionConsumerFifo', () => {
  describe('FIFO topic/queue consumption and retry', () => {
    const topicName = 'test-fifo-consumer.fifo'
    const queueName = 'test-fifo-consumer.fifo'
    const dlqName = 'test-fifo-consumer-dlq.fifo'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let testAdmin: TestAwsResourceAdmin
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      testAdmin = diContainer.cradle.testAdmin
    })

    afterEach(async () => {
      await testAdmin.deleteTopics(topicName)
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('creates FIFO topic/queue and consumes messages', async () => {
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      const consumer = new SnsSqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
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

      expect(consumer.subscriptionProps.queueName).toBe(queueName)

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
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
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
      const consumer = new SnsSqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 200, // Short interval for faster test execution
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

    it('preserves MessageGroupId during retry', async () => {
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
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
      const consumer = new SnsSqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        barrierSleepCheckIntervalInMsecs: 200, // Short interval for faster test execution
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

      // FIFO queues use barrier sleep approach - no retry republishing
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

      // No retry republishing for FIFO queues
      expect(retrySendCalls.length).toBe(0)

      // Verify message was processed twice (initial attempt + recheck after sleep)
      expect(attemptCount).toBe(2)

      await consumer.close(true)
    })

    it('sends failed messages to FIFO DLQ with MessageGroupId', async () => {
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        defaultMessageGroupId: 'test-group',
      })

      const consumer = new SnsSqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
              VisibilityTimeout: '1',
            },
          },
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'true',
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
        barrierSleepCheckIntervalInMsecs: 200, // Short interval for faster test execution
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
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
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
      const consumer = new SnsSqsPermissionConsumerFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'true',
            },
          },
          topic: {
            Name: topicName,
            Attributes: {
              FifoTopic: 'true',
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
  })
})
