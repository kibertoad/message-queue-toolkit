import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { PayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { isFifoQueueName, validateFifoQueueName } from '../../lib/utils/sqsUtils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SqsPermissionPublisherFifo } from './SqsPermissionPublisherFifo.ts'

describe('SqsPermissionPublisherFifo', () => {
  describe('FIFO queue validation', () => {
    it('validates FIFO queue names correctly', () => {
      expect(isFifoQueueName('my-queue.fifo')).toBe(true)
      expect(isFifoQueueName('my-queue')).toBe(false)
    })

    it('throws error when FIFO queue name does not end with .fifo', () => {
      expect(() => validateFifoQueueName('my-queue', true)).toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
    })

    it('throws error when non-FIFO queue name ends with .fifo', () => {
      expect(() => validateFifoQueueName('my-queue.fifo', false)).toThrow(
        /fifoQueue option is not set to true/,
      )
    })

    it('does not throw for valid FIFO queue name', () => {
      expect(() => validateFifoQueueName('my-queue.fifo', true)).not.toThrow()
    })

    it('does not throw for valid standard queue name', () => {
      expect(() => validateFifoQueueName('my-queue', false)).not.toThrow()
    })
  })

  describe('FIFO queue creation and publishing', () => {
    const queueName = 'test-fifo-queue.fifo'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let testAdmin: TestAwsResourceAdmin
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      testAdmin = diContainer.cradle.testAdmin
      await testAdmin.deleteQueues(queueName)
    })

    afterEach(async () => {
      await testAdmin.deleteQueues(queueName)
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('creates FIFO queue with correct attributes', async () => {
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
      })

      await publisher.init()

      expect(publisher.queueProps.name).toBe(queueName)
      expect(publisher.queueProps.isFifo).toBe(true)
    })

    it('throws error when fifoQueue is true but queue name does not end with .fifo', async () => {
      const publisher = new SqsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: 'invalid-queue-name',
            Attributes: {
              FifoQueue: 'true',
            },
          },
        },
      })

      await expect(() => publisher.init()).rejects.toThrow(
        /FIFO queue names must end with .fifo suffix/,
      )
    })

    it('publishes message with MessageGroupId from field', async () => {
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

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: 'user-group-1',
      })

      const sendCalls = sqsSpy.mock.calls.filter((call) => call[0] instanceof SendMessageCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as SendMessageCommand
      expect(command.input.MessageGroupId).toBe('user-group-1')
    })

    it('publishes message with default MessageGroupId', async () => {
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
        defaultMessageGroupId: 'default-group',
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: 'some-users',
      })

      const sendCalls = sqsSpy.mock.calls.filter((call) => call[0] instanceof SendMessageCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as SendMessageCommand
      expect(command.input.MessageGroupId).toBe('default-group')
    })

    it('publishes message with explicit MessageGroupId in options', async () => {
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
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await publisher.publish(
        {
          id: '1',
          messageType: 'add',
          userIds: 'some-users',
        },
        {
          MessageGroupId: 'explicit-group',
        },
      )

      const sendCalls = sqsSpy.mock.calls.filter((call) => call[0] instanceof SendMessageCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as SendMessageCommand
      expect(command.input.MessageGroupId).toBe('explicit-group')
    })

    it('throws error when MessageGroupId is not provided for FIFO queue', async () => {
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
      })

      await publisher.init()

      await expect(() =>
        publisher.publish({
          id: '1',
          messageType: 'add',
          userIds: 'some-users',
        }),
      ).rejects.toThrow(/MessageGroupId is required for FIFO queues/)
    })

    it('publishes message with MessageDeduplicationId', async () => {
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
        defaultMessageGroupId: 'default-group',
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await publisher.publish(
        {
          id: '1',
          messageType: 'add',
          userIds: 'some-users',
        },
        {
          MessageDeduplicationId: 'unique-dedup-id',
        },
      )

      const sendCalls = sqsSpy.mock.calls.filter((call) => call[0] instanceof SendMessageCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as SendMessageCommand
      expect(command.input.MessageDeduplicationId).toBe('unique-dedup-id')
    })

    it('resolves MessageGroupId from messageGroupIdField even when payload is offloaded', async () => {
      const s3BucketName = 'fifo-payload-offloading-test-bucket'
      await testAdmin.createBucket(s3BucketName)

      const payloadStoreConfig: PayloadStoreConfig = {
        messageSizeThreshold: 100, // Very small threshold to force offloading
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
        storeName: 's3',
      }

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
        messageGroupIdField: 'userIds', // MessageGroupId should come from this field
        payloadStoreConfig,
      })

      await publisher.init()

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      // Publish a large message that will trigger offloading
      // MessageGroupId must be <= 128 chars, but we can make the payload large with metadata
      const messageGroupId = 'user-group-123'
      const largePayload = 'x'.repeat(500) // Make payload large to exceed 100 byte threshold
      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: messageGroupId,
        metadata: {
          largeField: largePayload, // This will trigger offloading
        },
      })

      const sendCalls = sqsSpy.mock.calls.filter((call) => call[0] instanceof SendMessageCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as SendMessageCommand

      // MessageGroupId should be resolved from original message before offloading
      expect(command.input.MessageGroupId).toBe(messageGroupId)

      // Verify payload was actually offloaded
      const messageBody = JSON.parse(command.input.MessageBody || '{}')
      expect(messageBody.offloadedPayloadPointer).toBeDefined()
      expect(messageBody.offloadedPayloadSize).toBeGreaterThan(0)
    })
  })
})
