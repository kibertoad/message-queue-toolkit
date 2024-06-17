import type { Message, SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { OffloadedPayloadPointerPayload } from '@message-queue-toolkit/core/dist/lib/messages/offloadedPayloadMessageSchemas'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver'
import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '../../lib/sqs/AbstractSqsPublisher'
import type { SQSMessage } from '../../lib/types/MessageTypes'
import { deserializeSQSMessage } from '../../lib/utils/sqsMessageDeserializer'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakePayloadStore } from '../fakes/FakePayloadStore'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionPublisher } from './SqsPermissionPublisher'

describe('SqsPermissionPublisher', () => {
  describe('init', () => {
    const queueName = 'someQueue'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      await deleteQueue(sqsClient, queueName)
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('throws an error when invalid queue locator is passed', async () => {
      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
        },
      })

      await expect(() => newPublisher.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
        },
      })

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(
        `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
      )
    })

    it('updates existing queue when one with different attributes exist', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
        Attributes: {
          KmsMasterKeyId: 'somevalue',
        },
      })

      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              KmsMasterKeyId: 'othervalue',
            },
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: false,
        },
        logMessages: true,
      })

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeDefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newPublisher.queueProps.url,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('othervalue')
    })

    it('does not update existing queue when attributes did not change', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
        Attributes: {
          KmsMasterKeyId: 'somevalue',
        },
      })

      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: queueName,
            Attributes: {
              KmsMasterKeyId: 'somevalue',
            },
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: false,
        },
        logMessages: true,
      })

      const sqsSpy = vi.spyOn(sqsClient, 'send')

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeUndefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newPublisher.queueProps.url,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('somevalue')
    })
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let permissionPublisher: SqsPermissionPublisher

    beforeEach(async () => {
      diContainer = await registerDependencies()
      await diContainer.cradle.permissionConsumer.close()
      permissionPublisher = diContainer.cradle.permissionPublisher
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publish invalid message', async () => {
      await expect(
        permissionPublisher.publish({
          id: '10',
          messageType: 'bad' as any,
        }),
      ).rejects.toThrow(/Unsupported message type: bad/)
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      const spy = await permissionPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toBe('published')
    })

    it('publish a message auto-filling internal properties', async () => {
      const QueueName = 'auto-filling_test_queue'
      const { queueUrl } = await assertQueue(diContainer.cradle.sqsClient, {
        QueueName,
      })

      const permissionPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName },
        },
      })

      let receivedMessage: unknown
      const consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: async (message: SQSMessage) => {
          if (message === null) {
            return
          }
          const decodedMessage = deserializeSQSMessage(
            message as any,
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )
          receivedMessage = decodedMessage.result!
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)
      expect(receivedMessage).toEqual({
        originalMessage: {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
          _internalNumberOfRetries: 0,
        },
        parsedMessage: {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
        },
      })

      consumer.stop()
      await permissionPublisher.close()
    })

    it('publish message with lazy loading', async () => {
      const newPublisher = new SqsPermissionPublisher(diContainer.cradle)

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await newPublisher.publish(message)

      const spy = await newPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toBe('published')
    })

    describe('payload offloading', () => {
      const queueName = 'payload-offloading_test_queue'
      const largeMessageThreshold = 1024 // Messages larger than 1KB shall be offloaded
      let publisher: SqsPermissionPublisher
      let consumer: Consumer
      let receivedSqsMessages: Message[]
      let fakePayloadStore: FakePayloadStore

      beforeEach(async () => {
        await deleteQueue(diContainer.cradle.sqsClient, queueName)
        const { queueUrl } = await assertQueue(diContainer.cradle.sqsClient, {
          QueueName: queueName,
        })

        receivedSqsMessages = []
        consumer = Consumer.create({
          queueUrl,
          handleMessage: async (message: Message) => {
            if (message === null) {
              return
            }
            receivedSqsMessages.push(message)
          },
          sqs: diContainer.cradle.sqsClient,
          messageAttributeNames: [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE],
        })
        consumer.start()

        fakePayloadStore = new FakePayloadStore()
        publisher = new SqsPermissionPublisher(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          payloadStoreConfig: {
            messageSizeThreshold: largeMessageThreshold,
            store: fakePayloadStore,
          },
        })
      })
      afterEach(async () => {
        await publisher.close()
        consumer.stop()
      })

      it('offloads large message payload to payload store', async () => {
        const message = {
          id: '1',
          messageType: 'add',
          metadata: { largeField: 'a'.repeat(largeMessageThreshold) },
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

        await publisher.publish(message)

        await expect(
          publisher.handlerSpy.waitForMessageWithId('1', 'published'),
        ).resolves.toBeDefined()
        await waitAndRetry(() => receivedSqsMessages.length > 0)

        // Check that the published message's body is a pointer to the offloaded payload.
        expect(receivedSqsMessages.length).toBe(1)
        const parsedReceivedMessageBody = JSON.parse(receivedSqsMessages[0].Body!)
        expect(parsedReceivedMessageBody).toMatchObject({
          offloadedPayloadPointer: expect.any(String),
          offloadedPayloadSize: expect.any(Number), //The actual size of the offloaded message is larger than JSON.stringify(message) because of the additional metadata (timestamp, retry count) that is added internally.
        })

        // Check that the published message had offloaded payload indicator.
        const receivedMessageAttributes = receivedSqsMessages[0].MessageAttributes
        expect(receivedMessageAttributes).toBeDefined()
        expect(receivedMessageAttributes![OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]).toBeDefined()

        // Check that the offloaded payload is stored in the payload store.
        const offloadedPayloadPointer = (
          parsedReceivedMessageBody as OffloadedPayloadPointerPayload
        ).offloadedPayloadPointer
        const offloadedPayload = fakePayloadStore.getAndClearPayloads()[offloadedPayloadPointer]
        expect(offloadedPayload).toBeDefined()

        // Check that the offloaded payload is the same as the original message.
        const parsedOffloadedPayload = JSON.parse(offloadedPayload)
        expect(parsedOffloadedPayload).toMatchObject(message)
      })
    })
  })
})
