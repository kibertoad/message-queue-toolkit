import type { S3 } from '@aws-sdk/client-s3'
import type { Message, SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type {
  MultiPayloadStoreConfig,
  OffloadedPayloadPointerPayload,
} from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import { assertQueue, OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { getObjectContent } from '../utils/s3Utils.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

import { SqsPermissionPublisher } from './SqsPermissionPublisher.ts'

const queueName = 'multiStorePayloadOffloadingTestQueue'

describe('SqsPermissionPublisher - multi-store payload offloading', () => {
  describe('publish', () => {
    const largeMessageSizeThreshold = 1024 // Messages larger than 1KB shall be offloaded
    const s3BucketNameStore1 = 'payload-offloading-store1-bucket'
    const s3BucketNameStore2 = 'payload-offloading-store2-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let s3: S3
    let testAdmin: TestAwsResourceAdmin

    let consumer: Consumer
    let receivedSqsMessages: Message[]

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      sqsClient = diContainer.cradle.sqsClient
      s3 = diContainer.cradle.s3
      testAdmin = diContainer.cradle.testAdmin

      await testAdmin.createBucket(s3BucketNameStore1)
      await testAdmin.createBucket(s3BucketNameStore2)
    })
    beforeEach(async () => {
      await testAdmin.deleteQueues(queueName)
      const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })

      receivedSqsMessages = []
      consumer = Consumer.create({
        queueUrl,
        handleMessage: (message: Message) => {
          if (message === null) {
            return Promise.resolve(message)
          }
          receivedSqsMessages.push(message)
          return Promise.resolve(message)
        },
        sqs: sqsClient,
        messageAttributeNames: [OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE],
      })

      consumer.start()
    })
    afterEach(() => {
      consumer.stop()
    })
    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('multi-store config: offloads payload to configured outgoing store', async () => {
      const store1 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore1 })
      const store2 = new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketNameStore2 })

      const payloadStoreConfig: MultiPayloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        stores: {
          's3-us-east-1': store1,
          's3-eu-central-1': store2,
        },
        outgoingStore: 's3-eu-central-1', // Use store2 for outgoing messages
      } satisfies MultiPayloadStoreConfig

      const publisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        payloadStoreConfig,
      })

      await publisher.init()

      const message = {
        id: '2',
        messageType: 'add',
        metadata: { largeField: 'b'.repeat(largeMessageSizeThreshold) },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      await expect(
        publisher.handlerSpy.waitForMessageWithId('2', 'published'),
      ).resolves.toBeDefined()
      await waitAndRetry(() => receivedSqsMessages.length > 0)

      expect(receivedSqsMessages.length).toBe(1)
      const parsedReceivedMessageBody = JSON.parse(
        receivedSqsMessages[0]!.Body!,
      ) as OffloadedPayloadPointerPayload

      // Check that message contains new payloadRef with correct store name
      expect(parsedReceivedMessageBody.payloadRef).toBeDefined()
      expect(parsedReceivedMessageBody.payloadRef).toMatchObject({
        id: expect.any(String),
        store: 's3-eu-central-1', // Should use the outgoing store
        size: expect.any(Number),
      })

      // Make sure the payload was offloaded to the correct S3 bucket (store2)
      await expect(
        getObjectContent(s3, s3BucketNameStore2, parsedReceivedMessageBody.payloadRef!.id),
      ).resolves.toBeDefined()

      await publisher.close()
    })
  })
})
