import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import { assertQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils'
import type { SnsPermissionPublisherMultiSchema } from '../publishers/SnsPermissionPublisherMultiSchema'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsSqsPermissionConsumerMultiSchema } from './SnsSqsPermissionConsumerMultiSchema'

describe('SNS PermissionsConsumerMultiSchema', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies({}, false)
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
    })

    it('throws an error when invalid queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumerMultiSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          subscriptionArn: 'dummy',
          topicArn: 'dummy',
        },
      })

      await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const arn = await assertTopic(snsClient, {
        Name: 'existingTopic',
      })

      const newConsumer = new SnsSqsPermissionConsumerMultiSchema(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
        },
      })

      await newConsumer.init()
      expect(newConsumer.queueUrl).toBe(
        'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
      )
      expect(newConsumer.topicArn).toEqual(arn)
      expect(newConsumer.subscriptionArn).toBe(
        'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
      )
      await deleteTopic(snsClient, 'existingTopic')
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisherMultiSchema
    let consumer: SnsSqsPermissionConsumerMultiSchema
    beforeEach(async () => {
      diContainer = await registerDependencies()
      publisher = diContainer.cradle.permissionPublisherMultiSchema
      consumer = diContainer.cradle.permissionConsumerMultiSchema
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle

      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    describe('happy path', () => {
      it('Processes messages', async () => {
        await publisher.publish({
          messageType: 'add',
        })
        await publisher.publish({
          messageType: 'remove',
        })
        await publisher.publish({
          messageType: 'remove',
        })

        await waitAndRetry(() => {
          return consumer.addCounter === 1 && consumer.removeCounter === 2
        })

        expect(consumer.addBarrierCounter).toBe(3)
        expect(consumer.addCounter).toBe(1)
        expect(consumer.removeCounter).toBe(2)
      })
    })
  })
})
