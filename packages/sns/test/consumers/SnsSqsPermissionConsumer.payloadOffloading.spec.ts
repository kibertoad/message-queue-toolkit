import type { SinglePayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { SNS_MESSAGE_MAX_SIZE } from '../../lib/sns/AbstractSnsService.ts'
import { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas.ts'

describe('SnsSqsPermissionConsumer - single-store payload offloading', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = SNS_MESSAGE_MAX_SIZE
    const s3BucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let testAdmin: TestAwsResourceAdmin
    let payloadStoreConfig: SinglePayloadStoreConfig

    let publisher: SnsPermissionPublisher
    let consumer: SnsSqsPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
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
      consumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig,
      })
      publisher = new SnsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig,
      })

      await testAdmin.deleteQueues(SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME)
      await testAdmin.deleteTopics(SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME)

      await consumer.start()
      await publisher.init()
    })

    afterEach(async () => {
      await publisher.close()
      await consumer.close()
    })

    afterAll(async () => {
      await testAdmin.emptyBuckets(s3BucketName)

      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('consumes large message with offloaded payload', async () => {
      // Craft a message that is larger than the max message size
      const message = {
        id: '1',
        messageType: 'add',
        metadata: {
          largeField: 'a'.repeat(largeMessageSizeThreshold),
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      expect(JSON.stringify(message).length).toBeGreaterThan(largeMessageSizeThreshold)

      await publisher.publish(message)

      const consumptionResult = await consumer.handlerSpy.waitForMessageWithId(
        message.id,
        'consumed',
      )
      expect(consumptionResult.message).toMatchObject(message)
    }, 15000)
  })
})
