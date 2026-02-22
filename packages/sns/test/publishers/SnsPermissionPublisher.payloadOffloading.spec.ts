import type { S3 } from '@aws-sdk/client-s3'
import type { SNSClient } from '@aws-sdk/client-sns'
import type { Message, SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { waitAndRetry } from '@lokalise/node-core'
import type {
  OffloadedPayloadPointerPayload,
  SinglePayloadStoreConfig,
} from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { SNS_MESSAGE_BODY_SCHEMA } from '../../lib/types/MessageTypes.ts'
import { subscribeToTopic } from '../../lib/utils/snsSubscriber.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { getObjectContent } from '../utils/s3Utils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsPermissionPublisher } from './SnsPermissionPublisher.ts'

const queueName = 'payloadOffloadingTestQueue'

describe('SnsPermissionPublisher - single-store payload offloading', () => {
  describe('publish', () => {
    const largeMessageSizeThreshold = 1024 // Messages larger than 1KB shall be offloaded
    const s3BucketName = 'payload-offloading-test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    let stsClient: STSClient
    let s3: S3
    let testAdmin: TestAwsResourceAdmin

    let payloadStoreConfig: SinglePayloadStoreConfig
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
      stsClient = diContainer.cradle.stsClient
      s3 = diContainer.cradle.s3
      testAdmin = diContainer.cradle.testAdmin

      await testAdmin.createBucket(s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, {
          bucketName: s3BucketName,
        }),
        storeName: 's3',
      }
    })

    beforeEach(async () => {
      await testAdmin.deleteQueues(queueName)
      await testAdmin.deleteTopics(SnsPermissionPublisher.TOPIC_NAME)
      const { queueUrl } = await testAdmin.createQueue(queueName)
      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
        handleMessage: (message: Message) => {
          if (message !== null) receivedSnsMessages.push(message)
          return Promise.resolve(message)
        },
        sqs: sqsClient,
        messageAttributeNames: [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE],
      })
      publisher = new SnsPermissionPublisher(diContainer.cradle, {
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
      await waitAndRetry(() => receivedSnsMessages.length > 0)

      // Check that the published message's body is a pointer to the offloaded payload.
      expect(receivedSnsMessages.length).toBe(1)
      const snsMessageBodyParseResult = SNS_MESSAGE_BODY_SCHEMA.safeParse(
        JSON.parse(receivedSnsMessages[0]!.Body!),
      )
      expect(snsMessageBodyParseResult.success).toBe(true)

      const parsedSnsMessage = JSON.parse(
        snsMessageBodyParseResult.data!.Message,
      ) as OffloadedPayloadPointerPayload

      // Check that message contains new payloadRef format
      expect(parsedSnsMessage.payloadRef).toBeDefined()
      expect(parsedSnsMessage.payloadRef).toMatchObject({
        id: expect.any(String),
        store: 's3', // Matches the configured storeName
        size: expect.any(Number),
      })

      // Check that legacy fields are also present for backward compatibility
      expect(parsedSnsMessage.offloadedPayloadPointer).toBe(parsedSnsMessage.payloadRef!.id)
      expect(parsedSnsMessage.offloadedPayloadSize).toBe(parsedSnsMessage.payloadRef!.size)

      // Check that the published message had offloaded payload indicator
      const receivedMessageAttributes = snsMessageBodyParseResult.data!.MessageAttributes
      expect(receivedMessageAttributes).toBeDefined()
      expect(receivedMessageAttributes![OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE]).toBeDefined()

      // Make sure the payload was offloaded to the S3 bucket
      await expect(
        getObjectContent(s3, s3BucketName, parsedSnsMessage.payloadRef!.id),
      ).resolves.toBeDefined()
    })
  })
})
