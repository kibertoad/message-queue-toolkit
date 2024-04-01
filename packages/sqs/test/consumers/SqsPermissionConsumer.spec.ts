/* eslint-disable max-lines */ // TODO: fix
import type { SendMessageCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand, ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { BarrierResult } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'
import { ZodError } from 'zod'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import { FakeLogger } from '../fakes/FakeLogger'
import type { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionConsumer } from './SqsPermissionConsumer'

describe('SqsPermissionConsumer', () => {
  describe('init', () => {
    const queueName = 'myTestQueue'

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
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
        },
      })

      await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
        },
      })

      await newConsumer.init()
      expect(newConsumer.queueProps.url).toBe(
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

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
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

      await newConsumer.init()
      expect(newConsumer.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeDefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.queueProps.url,
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

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
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

      await newConsumer.init()
      expect(newConsumer.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeUndefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.queueProps.url,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('somevalue')
    })
  })

  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisher

    beforeEach(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies({
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('logs a message when logging is enabled', async () => {
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        logMessages: true,
      })
      await newConsumer.start()

      await publisher.publish({
        id: '1',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      expect(logger.loggedMessages.length).toBe(2)
      expect(logger.loggedMessages).toMatchInlineSnapshot(`
        [
          {
            "id": "1",
            "messageType": "add",
          },
          {
            "messageId": "1",
            "processingResult": "consumed",
          },
        ]
      `)
      await newConsumer.close()
    })
  })

  describe('preHandlerBarrier', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisher
    beforeEach(async () => {
      diContainer = await registerDependencies()
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('blocks first try', async () => {
      let barrierCounter = 0
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        addPreHandlerBarrier: async (_msg): Promise<BarrierResult<number>> => {
          barrierCounter++
          if (barrierCounter < 2) {
            return {
              isPassing: false,
            }
          }

          return { isPassing: true, output: barrierCounter }
        },
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
      await newConsumer.close()
    })

    it('can access prehandler output', async () => {
      expect.assertions(1)
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        addPreHandlerBarrier: async (
          message,
          executionContext,
          prehandlerOutput,
        ): Promise<BarrierResult<number>> => {
          expect(prehandlerOutput.messageId).toBe(message.id)

          return { isPassing: true, output: 1 }
        },
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      await newConsumer.close()
    })

    it('throws an error on first try', async () => {
      let barrierCounter = 0
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          if (barrierCounter === 1) {
            throw new Error()
          }
          return Promise.resolve({ isPassing: true, output: barrierCounter })
        },
      })
      await newConsumer.start()

      await publisher.publish({
        id: '3',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('3', 'consumed')

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
      await newConsumer.close()
    })
  })

  describe('prehandlers', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisher
    beforeEach(async () => {
      diContainer = await registerDependencies()
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('processes one prehandler', async () => {
      expect.assertions(1)

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.messageId).toEqual(message.id)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.messageId = message.id
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })

    it('processes two prehandlers', async () => {
      expect.assertions(1)

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.messageId).toEqual(message.id + ' adjusted')
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.messageId = message.id
            next({
              result: 'success',
            })
          },

          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.messageId += ' adjusted'
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer
    let errorResolver: FakeConsumerErrorResolver

    beforeEach(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer

      const command = new ReceiveMessageCommand({
        QueueUrl: publisher.queueProps.url,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()

      errorResolver = diContainer.cradle.consumerErrorResolver as FakeConsumerErrorResolver
      errorResolver.clear()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('bad event', async () => {
      const message = {
        messageType: 'add',
      }

      // not using publisher to avoid publisher validation
      const input = {
        QueueUrl: consumer.queueProps.url,
        MessageBody: JSON.stringify(message),
      } satisfies SendMessageCommandInput
      const command = new SendMessageCommand(input)
      await sqsClient.send(command)

      await waitAndRetry(() => errorResolver.errors.length > 0, 100, 5)

      expect(errorResolver.errors).toHaveLength(1)
      expect(errorResolver.errors[0] instanceof ZodError).toBe(true)

      expect(consumer.addCounter).toBe(0)
      expect(consumer.removeCounter).toBe(0)
    })

    it('Processes messages', async () => {
      await publisher.publish({
        id: '10',
        messageType: 'add',
      })
      await publisher.publish({
        id: '20',
        messageType: 'remove',
      })
      await publisher.publish({
        id: '30',
        messageType: 'remove',
      })

      await consumer.handlerSpy.waitForMessageWithId('10', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('20', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('30', 'consumed')

      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(2)
    })
  })

  describe('dead letter queue', () => {
    describe('init', () => {
      const customDlqName = 'customDlq'
      const customSuffix = '-customSuffix'
      const queueName = 'sqsTestQueue'

      let diContainer: AwilixContainer<Dependencies>
      let sqsClient: SQSClient

      beforeAll(async () => {
        diContainer = await registerDependencies()
        sqsClient = diContainer.cradle.sqsClient
      })

      beforeEach(async () => {
        await deleteQueue(sqsClient, queueName)
        await deleteQueue(sqsClient, customDlqName)
        await deleteQueue(sqsClient, `${queueName}-dlq`)
        await deleteQueue(sqsClient, `${queueName}${customSuffix}`)
      })

      afterAll(async () => {
        await diContainer.cradle.awilixManager.executeDispose()
        await diContainer.dispose()
      })

      it('creates dead letter queue using default name', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: { redrivePolicy: { maxReceiveCount: 5 } },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}-dlq`,
        )
        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${queueName}-dlq`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue using custom suffix', async () => {
        const suffix = '-test'
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { queueNameSuffix: suffix } },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}${suffix}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${queueName}${suffix}`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue using custom name', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { QueueName: customDlqName } },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customDlqName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDlqName}`,
            maxReceiveCount: 5,
          }),
        })
      })

      // TODO: add test cases for locator and update
    })

    // eslint-disable-next-line vitest/no-commented-out-tests
    /*
    describe('messages with errors on process should go to DLQ', () => {
      let diContainer: AwilixContainer<Dependencies>
      let publisher: SqsPermissionPublisherMultiSchema
      let sqsClient: SQSClient

      beforeAll(async () => {
        diContainer = await registerDependencies()
        // eslint-disable-next-line max-lines
        sqsClient = diContainer.cradle.sqsClient
        publisher = diContainer.cradle.permissionPublisherMultiSchema
      })

      afterEach(async () => {
        await diContainer.cradle.awilixManager.executeDispose()
        await diContainer.dispose()
      })

      beforeEach(async () => {
        await deleteQueue(sqsClient, publisher.queueName)
        await deleteQueue(sqsClient, `${publisher.queueName}-dlq`)
      })

      it('after errors, messages should go to DLQ', async () => {
        let counter = 0
        const consumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
          creationConfig: { queue: { QueueName: publisher.queueName } },
          deadLetterQueue: { redrivePolicy: { maxReceiveCount: 2 } },

          removeHandlerOverride: async () => {
            counter++
            throw new Error('Error')
          },
        })
        await consumer.start()

        let dlqMessage: any
        const dlqConsumer = Consumer.create({
          sqs: diContainer.cradle.sqsClient,
          queueUrl: consumer.deadLetterQueueUrl!,
          handleMessage: async (message: SQSMessage) => {
            dlqMessage = message
          },
        })
        dlqConsumer.start()

        await publisher.publish({ id: '1', messageType: 'remove' })

        await waitAndRetry(async () => dlqMessage, 20, 5)

        expect(counter).toBe(2)
        expect(dlqMessage.Body).toBe(JSON.stringify({ id: '1', messageType: 'remove' }))
      })
    })
     */
  })
})
