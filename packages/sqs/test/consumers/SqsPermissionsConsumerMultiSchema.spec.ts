import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, afterAll, beforeAll } from 'vitest'

import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import type { SqsPermissionPublisherMultiSchema } from '../publishers/SqsPermissionPublisherMultiSchema'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionConsumerMultiSchema } from './SqsPermissionConsumerMultiSchema'

describe('SqsPermissionsConsumerMultiSchema', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      await deleteQueue(sqsClient, 'existingQueue')
    })

    it('throws an error when invalid queue locator is passed', async () => {
      const newConsumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
        },
      })

      await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
        },
      })

      await newConsumer.init()
      expect(newConsumer.queueUrl).toBe(
        'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
      )
    })
  })

  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisherMultiSchema

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
      const newConsumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueName,
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
    let publisher: SqsPermissionPublisherMultiSchema

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
      const newConsumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueName,
          },
        },
        addPreHandlerBarrier: async (_msg) => {
          barrierCounter++
          return barrierCounter > 1
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
      const newConsumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueName,
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
    let publisher: SqsPermissionPublisherMultiSchema
    let consumer: SqsPermissionConsumerMultiSchema
    let sqsClient: SQSClient
    beforeAll(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisherMultiSchema
      consumer = diContainer.cradle.permissionConsumerMultiSchema
    })

    beforeEach(async () => {
      await consumer.start()

      const command = new ReceiveMessageCommand({
        QueueUrl: publisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()

      const fakeErrorResolver = diContainer.cradle
        .consumerErrorResolver as FakeConsumerErrorResolver
      fakeErrorResolver.clear()
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      await diContainer.cradle.permissionConsumerMultiSchema.close()
      await diContainer.cradle.permissionConsumerMultiSchema.close(true)
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
