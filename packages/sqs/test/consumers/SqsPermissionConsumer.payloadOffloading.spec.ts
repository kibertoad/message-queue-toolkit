import type { S3 } from '@aws-sdk/client-s3'
import type { PayloadStoreConfig } from '@message-queue-toolkit/core'
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { SQS_MESSAGE_MAX_SIZE } from '../../lib/sqs/AbstractSqsService'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import { assertBucket, emptyBucket } from '../utils/s3Utils'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionConsumer } from './SqsPermissionConsumer'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas'

describe('SqsPermissionConsumer', () => {
  describe('consume', () => {
    const largeMessageSizeThreshold = SQS_MESSAGE_MAX_SIZE
    const s3BucketName = 'test-bucket'

    let diContainer: AwilixContainer<Dependencies>
    let s3: S3
    let payloadStoreConfig: PayloadStoreConfig

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      s3 = diContainer.cradle.s3

      await assertBucket(s3, s3BucketName)
      payloadStoreConfig = {
        messageSizeThreshold: largeMessageSizeThreshold,
        store: new S3PayloadStore(diContainer.cradle, { bucketName: s3BucketName }),
      }
    })
    beforeEach(async () => {
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        payloadStoreConfig,
      })
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        payloadStoreConfig,
      })
      await consumer.start()
      await publisher.init()
    })
    afterEach(async () => {
      await publisher.close()
      await consumer.close()
    })
    afterAll(async () => {
      await emptyBucket(s3, s3BucketName)

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
    })
  })
})
