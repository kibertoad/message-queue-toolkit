import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import { assertQueue } from '@message-queue-toolkit/sqs'
import { SqsPermissionConsumerMultiSchema } from '@message-queue-toolkit/sqs/dist/test/consumers/SqsPermissionConsumerMultiSchema'
import { FakeLogger } from '@message-queue-toolkit/sqs/dist/test/fakes/FakeLogger'
import type { SqsPermissionPublisherMultiSchema } from '@message-queue-toolkit/sqs/dist/test/publishers/SqsPermissionPublisherMultiSchema'
import type { AwilixContainer } from 'awilix'
import { asFunction } from 'awilix'
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

  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisherMultiSchema

    beforeEach(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies({
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumerMultiSchema.close()
      publisher = diContainer.cradle.permissionPublisherMultiSchema
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('logs a message when logging is enabled', async () => {
      const newConsumer = new SnsSqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: SnsSqsPermissionConsumerMultiSchema.CONSUMED_QUEUE_NAME,
          },
          topic: {
            Name: SnsSqsPermissionConsumerMultiSchema.SUBSCRIBED_TOPIC_NAME,
          },
        },
        logMessages: true,
      })
      await newConsumer.start()

      await publisher.publish({
        messageType: 'add',
      })

      await waitAndRetry(() => {
        return logger.loggedMessages.length === 1
      })

      expect(logger.loggedMessages.length).toBe(1)
    })
  })

  describe('preHandlerBarrier', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisherMultiSchema

    beforeEach(async () => {
      diContainer = await registerDependencies()
      await diContainer.cradle.permissionConsumerMultiSchema.close()
      publisher = diContainer.cradle.permissionPublisherMultiSchema
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('blocks first try', async () => {
      let barrierCounter = 0
      const newConsumer = new SnsSqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: SnsSqsPermissionConsumerMultiSchema.CONSUMED_QUEUE_NAME,
          },
          topic: {
            Name: SnsSqsPermissionConsumerMultiSchema.SUBSCRIBED_TOPIC_NAME,
          },
        },
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          return Promise.resolve(barrierCounter > 1)
        },
      })
      await newConsumer.start()

      await publisher.publish({
        messageType: 'add',
      })

      await waitAndRetry(() => {
        return newConsumer.addCounter === 1
      })

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
    })

    it('throws an error on first try', async () => {
      let barrierCounter = 0
      const newConsumer = new SnsSqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: SnsSqsPermissionConsumerMultiSchema.CONSUMED_QUEUE_NAME,
          },
          topic: {
            Name: SnsSqsPermissionConsumerMultiSchema.SUBSCRIBED_TOPIC_NAME,
          },
        },
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          if (barrierCounter === 1) {
            throw new Error()
          }
          return Promise.resolve(true)
        },
      })
      await newConsumer.start()

      await publisher.publish({
        messageType: 'add',
      })

      await waitAndRetry(() => {
        return newConsumer.addCounter === 1
      })

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
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

        expect(consumer.addCounter).toBe(1)
        expect(consumer.removeCounter).toBe(2)
      })
    })
  })
})
