import type { S3 } from '@aws-sdk/client-s3'
import type { SNSClient } from '@aws-sdk/client-sns'
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
import type { AwilixContainer } from 'awilix';
import { asValue } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { SNS_MESSAGE_BODY_SCHEMA } from '../../lib/types/MessageTypes'
import { subscribeToTopic } from '../../lib/utils/snsSubscriber'
import { deleteTopic } from '../../lib/utils/snsUtils'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { assertBucket, getObjectContent } from '../utils/s3Utils'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsPermissionPublisher } from './SnsPermissionPublisher'

const queueName = 'payloadOffloadingTestQueue'

describe('SnsPermissionPublisher', () => {
  describe('publish', () => {
    const largeMessageSizeThreshold = 1024 // Messages larger than 1KB shall be offloaded
    const s3BucketName = 'payload-offloading-test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    let s3: S3

    let payloadStoreConfig: PayloadStoreConfig
    let publisher: SnsPermissionPublisher
    let consumer: Consumer
    let receivedSnsMessages: Message[]

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      s3 = diContainer.cradle.s3

      await assertBucket(s3, s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
      }
    })
    beforeEach(async () => {
      await deleteQueue(sqsClient, queueName)
      await deleteTopic(snsClient, SnsPermissionPublisher.TOPIC_NAME)
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })
      await subscribeToTopic(
        sqsClient,
        snsClient,
        { QueueName: queueName },
        {
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
        {
          updateAttributesIfExists: false,
        },
      )

      receivedSnsMessages = []
      consumer = Consumer.create({
        queueUrl,
        handleMessage: async (message: Message) => {
          if (message === null) {
            return
          }
          receivedSnsMessages.push(message)
        },
        sqs: sqsClient,
        messageAttributeNames: [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE],
      })
      publisher = new SnsPermissionPublisher(diContainer.cradle, { payloadStoreConfig })

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
      await waitAndRetry(() => receivedSnsMessages.length > 0)

      // Check that the published message's body is a pointer to the offloaded payload.
      expect(receivedSnsMessages.length).toBe(1)
      const snsMessageBodyParseResult = SNS_MESSAGE_BODY_SCHEMA.safeParse(
        JSON.parse(receivedSnsMessages[0].Body!),
      )
      expect(snsMessageBodyParseResult.success).toBe(true)

      const parsedSqsMessage = JSON.parse(snsMessageBodyParseResult.data!.Message)
      expect(parsedSqsMessage).toMatchObject({
        offloadedPayloadPointer: expect.any(String),
        offloadedPayloadSize: expect.any(Number), //The actual size of the offloaded message is larger than JSON.stringify(message) because of the additional metadata (timestamp, retry count) that is added internally.
      })

      // Check that the published message had offloaded payload indicator.
      const receivedMessageAttributes = snsMessageBodyParseResult.data!.MessageAttributes
      expect(receivedMessageAttributes).toBeDefined()
      expect(receivedMessageAttributes![OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]).toBeDefined()

      // Make sure the payload was offloaded to the S3 bucket.
      const offloadedPayloadPointer = (parsedSqsMessage as OffloadedPayloadPointerPayload)
        .offloadedPayloadPointer
      await expect(
        getObjectContent(s3, s3BucketName, offloadedPayloadPointer),
      ).resolves.toBeDefined()
    })
  })
})
