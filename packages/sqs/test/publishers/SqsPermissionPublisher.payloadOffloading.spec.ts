import type { S3 } from '@aws-sdk/client-s3'
import type { Message, SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type {
  OffloadedPayloadPointerPayload,
  PayloadStoreConfig,
} from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import {
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
  assertQueue,
  deleteQueue,
} from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { assertBucket, getObjectContent } from '../utils/s3Utils.ts'
import { registerDependencies } from '../utils/testContext.ts'
import type { Dependencies } from '../utils/testContext.ts'

import { SqsPermissionPublisher } from './SqsPermissionPublisher.ts'

const queueName = 'payloadOffloadingTestQueue'

describe('SqsPermissionPublisher - payload offloading', () => {
  describe('publish', () => {
    const largeMessageSizeThreshold = 1024 // Messages larger than 1KB shall be offloaded
    const s3BucketName = 'payload-offloading-test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let s3: S3

    let payloadStoreConfig: PayloadStoreConfig
    let publisher: SqsPermissionPublisher
    let consumer: Consumer
    let receivedSqsMessages: Message[]

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      sqsClient = diContainer.cradle.sqsClient
      s3 = diContainer.cradle.s3

      await assertBucket(s3, s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
      }
    })
    beforeEach(async () => {
      await deleteQueue(sqsClient, queueName)
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })

      receivedSqsMessages = []
      consumer = Consumer.create({
        queueUrl,
        handleMessage: (message: Message) => {
          if (message === null) {
            return Promise.resolve()
          }
          receivedSqsMessages.push(message)
          return Promise.resolve()
        },
        sqs: sqsClient,
        messageAttributeNames: [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE],
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        payloadStoreConfig,
      })

      consumer.start()
      await publisher.init()
    })
    afterEach(async () => {
      await publisher.close()
      consumer.stop()
    })
    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('offloads large message payload to payload store', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        metadata: { largeField: 'a'.repeat(largeMessageSizeThreshold) },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

      await publisher.publish(message)

      await expect(
        publisher.handlerSpy.waitForMessageWithId('1', 'published'),
      ).resolves.toBeDefined()
      await waitAndRetry(() => receivedSqsMessages.length > 0)

      // Check that the published message's body is a pointer to the offloaded payload.
      // Check that the published message's body is a pointer to the offloaded payload.
      expect(receivedSqsMessages.length).toBe(1)
      const parsedReceivedMessageBody = JSON.parse(receivedSqsMessages[0]!.Body!)
      expect(parsedReceivedMessageBody).toMatchObject({
        offloadedPayloadPointer: expect.any(String),
        offloadedPayloadSize: expect.any(Number), //The actual size of the offloaded message is larger than JSON.stringify(message) because of the additional metadata (timestamp, retry count) that is added internally.
      })

      // Check that the published message had offloaded payload indicator.
      const receivedMessageAttributes = receivedSqsMessages[0]!.MessageAttributes
      expect(receivedMessageAttributes).toBeDefined()
      expect(receivedMessageAttributes![OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]).toBeDefined()

      // Make sure the payload was offloaded to the S3 bucket.
      const offloadedPayloadPointer = (parsedReceivedMessageBody as OffloadedPayloadPointerPayload)
        .offloadedPayloadPointer
      await expect(
        getObjectContent(s3, s3BucketName, offloadedPayloadPointer),
      ).resolves.toBeDefined()
    })
  })
})
