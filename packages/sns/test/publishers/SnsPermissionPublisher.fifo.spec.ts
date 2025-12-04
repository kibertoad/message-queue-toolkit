import type { SNSClient } from '@aws-sdk/client-sns'
import { PublishCommand } from '@aws-sdk/client-sns'
import type { SinglePayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { deleteTopic, isFifoTopicName, validateFifoTopicName } from '../../lib/utils/snsUtils.ts'
import { assertBucket } from '../utils/s3Utils.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsPermissionPublisherFifo } from './SnsPermissionPublisherFifo.ts'

describe('SnsPermissionPublisherFifo', () => {
  describe('FIFO topic validation', () => {
    it('validates FIFO topic names correctly', () => {
      expect(isFifoTopicName('my-topic.fifo')).toBe(true)
      expect(isFifoTopicName('my-topic')).toBe(false)
    })

    it('throws error when FIFO topic name does not end with .fifo', () => {
      expect(() => validateFifoTopicName('my-topic', true)).toThrow(
        /FIFO topic names must end with .fifo suffix/,
      )
    })

    it('throws error when non-FIFO topic name ends with .fifo', () => {
      expect(() => validateFifoTopicName('my-topic.fifo', false)).toThrow(
        /fifoTopic option is not set to true/,
      )
    })

    it('does not throw for valid FIFO topic name', () => {
      expect(() => validateFifoTopicName('my-topic.fifo', true)).not.toThrow()
    })

    it('does not throw for valid standard topic name', () => {
      expect(() => validateFifoTopicName('my-topic', false)).not.toThrow()
    })
  })

  describe('FIFO topic creation and publishing', () => {
    const topicName = 'test-fifo-topic.fifo'

    let diContainer: AwilixContainer<Dependencies>
    let snsClient: SNSClient
    beforeEach(async () => {
      diContainer = await registerDependencies()
      snsClient = diContainer.cradle.snsClient
      await deleteTopic(snsClient, diContainer.cradle.stsClient, topicName)
    })

    afterEach(async () => {
      await deleteTopic(snsClient, diContainer.cradle.stsClient, topicName)
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('creates FIFO topic with correct attributes', async () => {
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
      })

      await publisher.init()

      expect(publisher.topicArnProp).toContain(topicName)
    })

    it('throws error when fifoTopic is true but topic name does not end with .fifo', async () => {
      const publisher = new SnsPermissionPublisherFifo(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'invalid-topic-name',
            Attributes: {
              FifoTopic: 'true',
            },
          },
        },
      })

      await expect(() => publisher.init()).rejects.toThrow(
        /FIFO topic names must end with .fifo|Fifo Topic names/,
      )
    })

    it('publishes message with MessageGroupId from field', async () => {
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

      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: 'user-group-1',
      })

      const sendCalls = snsSpy.mock.calls.filter((call) => call[0] instanceof PublishCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as PublishCommand
      expect(command.input.MessageGroupId).toBe('user-group-1')
    })

    it('publishes message with default MessageGroupId', async () => {
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
        defaultMessageGroupId: 'default-group',
      })

      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

      await publisher.publish({
        id: '1',
        messageType: 'add',
        userIds: 'some-users',
      })

      const sendCalls = snsSpy.mock.calls.filter((call) => call[0] instanceof PublishCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as PublishCommand
      expect(command.input.MessageGroupId).toBe('default-group')
    })

    it('publishes message with explicit MessageGroupId in options', async () => {
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
      })

      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

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

      const sendCalls = snsSpy.mock.calls.filter((call) => call[0] instanceof PublishCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as PublishCommand
      expect(command.input.MessageGroupId).toBe('explicit-group')
    })

    it('throws error when MessageGroupId is not provided for FIFO topic', async () => {
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
      })

      await publisher.init()

      await expect(() =>
        publisher.publish({
          id: '1',
          messageType: 'add',
          userIds: 'some-users',
        }),
      ).rejects.toThrow(/MessageGroupId is required for FIFO topics/)
    })

    it('publishes message with MessageDeduplicationId', async () => {
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
        defaultMessageGroupId: 'default-group',
      })

      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

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

      const sendCalls = snsSpy.mock.calls.filter((call) => call[0] instanceof PublishCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as PublishCommand
      expect(command.input.MessageDeduplicationId).toBe('unique-dedup-id')
    })

    it('resolves MessageGroupId from messageGroupIdField even when payload is offloaded', async () => {
      const s3BucketName = 'fifo-payload-offloading-test-bucket'
      const s3 = diContainer.cradle.s3
      await assertBucket(s3, s3BucketName)

      const payloadStoreConfig: SinglePayloadStoreConfig = {
        messageSizeThreshold: 100, // Very small threshold to force offloading
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
        storeName: 's3',
      }

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
        messageGroupIdField: 'userIds', // MessageGroupId should come from this field
        payloadStoreConfig,
      })

      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

      // Publish a large message that will trigger offloading
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

      const sendCalls = snsSpy.mock.calls.filter((call) => call[0] instanceof PublishCommand)
      expect(sendCalls.length).toBeGreaterThan(0)

      const lastSendCall = sendCalls[sendCalls.length - 1]
      const command = lastSendCall?.[0] as PublishCommand

      // MessageGroupId should be resolved from original message before offloading
      expect(command.input.MessageGroupId).toBe(messageGroupId)

      // Verify payload was actually offloaded
      const messageBody = JSON.parse(command.input.Message || '{}')
      expect(messageBody.offloadedPayloadPointer).toBeDefined()
      expect(messageBody.offloadedPayloadSize).toBeGreaterThan(0)
    })
  })
})
