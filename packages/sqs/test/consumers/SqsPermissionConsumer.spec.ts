import { setTimeout } from 'node:timers/promises'

import {
  ListQueueTagsCommand,
  type SQSClient,
  type SendMessageCommandInput,
} from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { BarrierResult, ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction, asValue } from 'awilix'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ZodError } from 'zod'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import { FakeLogger } from '../fakes/FakeLogger'
import { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import { SINGLETON_CONFIG, registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionConsumer, type SupportedMessages } from './SqsPermissionConsumer'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from './userConsumerSchemas'

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

    describe('attributes update', () => {
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
                VisibilityTimeout: '10',
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

        const attributes = await getQueueAttributes(sqsClient, newConsumer.queueProps.url)

        expect(attributes.result?.attributes).toMatchObject({
          KmsMasterKeyId: 'othervalue',
          VisibilityTimeout: '10',
        })
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

        const attributes = await getQueueAttributes(sqsClient, newConsumer.queueProps.url)

        expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('somevalue')
      })
    })

    describe('tags update', () => {
      const getTags = (queueUrl: string) =>
        sqsClient.send(new ListQueueTagsCommand({ QueueUrl: queueUrl }))

      it('updates existing queue tags when update is forced', async () => {
        const initialTags = {
          project: 'some-project',
          service: 'some-service',
          leftover: 'some-leftover',
        }
        const newTags = {
          project: 'some-project',
          service: 'changed-service',
          cc: 'some-cc',
        }

        const assertResult = await assertQueue(sqsClient, {
          QueueName: queueName,
          tags: initialTags,
        })
        const preTags = await getTags(assertResult.queueUrl)
        expect(preTags.Tags).toEqual(initialTags)

        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              tags: newTags,
            },
            forceTagUpdate: true,
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
          return entry[0].constructor.name === 'TagQueueCommand'
        })
        expect(updateCall).toBeDefined()

        const postTags = await getTags(assertResult.queueUrl)
        expect(postTags.Tags).toEqual({
          ...newTags,
          leftover: 'some-leftover',
        })
      })

      it('does not update existing queue tags when update is not forced', async () => {
        const assertResult = await assertQueue(sqsClient, {
          QueueName: queueName,
          tags: {
            project: 'some-project',
            service: 'some-service',
            leftover: 'some-leftover',
          },
        })
        const preTags = await getTags(assertResult.queueUrl)
        expect(preTags.Tags).toEqual({
          project: 'some-project',
          service: 'some-service',
          leftover: 'some-leftover',
        })

        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              tags: {
                project: 'some-project',
                service: 'changed-service',
                cc: 'some-cc',
              },
            },
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
          return entry[0].constructor.name === 'TagQueueCommand'
        })
        expect(updateCall).toBeUndefined()

        const postTags = await getTags(assertResult.queueUrl)
        expect(postTags.Tags).toEqual({
          project: 'some-project',
          service: 'some-service',
          leftover: 'some-leftover',
        })
      })
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
      expect(logger.loggedMessages).toMatchObject([
        {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
        },
        {
          messageId: '1',
          processingResult: { status: 'consumed' },
        },
      ])
      await newConsumer.close()
    })
  })

  describe('metrics', () => {
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

    it('registers metrics if metrics manager is provided', async () => {
      const messagesRegisteredInMetrics: ProcessedMessageMetadata<SupportedMessages>[] = []
      const newConsumer = new SqsPermissionConsumer({
        ...diContainer.cradle,
        messageMetricsManager: {
          registerProcessedMessage(metadata: ProcessedMessageMetadata<SupportedMessages>): void {
            messagesRegisteredInMetrics.push(metadata)
          },
        },
      })
      await newConsumer.start()

      publisher.publish({
        id: '1',
        messageType: 'add',
        metadata: {
          schemaVersions: '1.0.0',
        },
      })

      await newConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      await newConsumer.close()

      expect(messagesRegisteredInMetrics).toStrictEqual([
        {
          messageId: '1',
          messageType: 'add',
          messageDeduplicationId: undefined,
          processingResult: { status: 'consumed' },
          messageTimestamp: expect.any(Number),
          messageProcessingStartTimestamp: expect.any(Number),
          messageProcessingEndTimestamp: expect.any(Number),
          queueName: SqsPermissionConsumer.QUEUE_NAME,
          message: expect.objectContaining({
            id: '1',
            messageType: 'add',
            metadata: { schemaVersions: '1.0.0' },
          }),
        },
      ])
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
        addPreHandlerBarrier: (_msg): Promise<BarrierResult<number>> => {
          barrierCounter++
          if (barrierCounter < 2) {
            return Promise.resolve({
              isPassing: false,
            })
          }

          return Promise.resolve({ isPassing: true, output: barrierCounter })
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

    it('can access preHandler output', async () => {
      expect.assertions(1)
      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        addPreHandlerBarrier: (
          message,
          _executionContext,
          preHandlerOutput,
        ): Promise<BarrierResult<number>> => {
          expect(preHandlerOutput.messageId).toBe(message.id)

          return Promise.resolve({ isPassing: true, output: 1 })
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

  describe('preHandlers', () => {
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

    it('processes one preHandler', async () => {
      expect.assertions(1)

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        removeHandlerOverride: (message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.messageId).toEqual(message.id)
          return Promise.resolve({
            result: 'success',
          })
        },
        removePreHandlers: [
          (message, _context, preHandlerOutput, next) => {
            preHandlerOutput.messageId = message.id
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

    it('processes two preHandlers', async () => {
      expect.assertions(1)

      const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: publisher.queueProps.name,
          },
        },
        removeHandlerOverride: (message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.messageId).toEqual(`${message.id} adjusted`)
          return Promise.resolve({
            result: 'success',
          })
        },
        removePreHandlers: [
          (message, _context, preHandlerOutput, next) => {
            preHandlerOutput.messageId = message.id
            next({
              result: 'success',
            })
          },

          (_message, _context, preHandlerOutput, next) => {
            preHandlerOutput.messageId += ' adjusted'
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

  describe('multiple consumers', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient

    let publisher: SqsPermissionPublisher
    let consumer: SqsPermissionConsumer

    beforeEach(async () => {
      diContainer = await registerDependencies({
        permissionConsumer: asFunction((dependencies) => {
          return new SqsPermissionConsumer(dependencies, {
            creationConfig: {
              queue: {
                QueueName: SqsPermissionConsumer.QUEUE_NAME,
              },
            },
            concurrentConsumersAmount: 5,
          })
        }),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer

      await consumer.start()

      const command = new ReceiveMessageCommand({
        QueueUrl: publisher.queueProps.url,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('process all messages properly', async () => {
      const messagesAmount = 100
      const messages: PERMISSIONS_ADD_MESSAGE_TYPE[] = Array.from({ length: messagesAmount }).map(
        (_, i) => ({
          id: `${i}`,
          messageType: 'add',
          timestamp: new Date().toISOString(),
        }),
      )

      messages.map((m) => publisher.publish(m))
      await Promise.all(
        messages.map((m) => consumer.handlerSpy.waitForMessageWithId(m.id, 'consumed')),
      )

      // Verifies that each message is executed only once
      expect(consumer.addCounter).toBe(messagesAmount)
      // Verifies that no message is lost
      expect(consumer.processedMessagesIds).toHaveLength(messagesAmount)
    })
  })

  describe('visibility timeout', () => {
    const queueName = 'myTestQueue'
    let diContainer: AwilixContainer<Dependencies>

    beforeEach(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('heartbeatInterval should be less than visibilityTimeout', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName, Attributes: { VisibilityTimeout: '1' } } },
        consumerOverrides: { heartbeatInterval: 2 },
      })
      await expect(() => consumer.start()).rejects.toThrow(
        /heartbeatInterval must be less than visibilityTimeout/,
      )
    })

    it.each([false, true])(
      'using 2 consumers with heartbeat -> %s',
      async (heartbeatEnabled) => {
        let consumer1IsProcessing = false
        let consumer1Counter = 0
        let consumer2Counter = 0

        const consumer1 = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: {
            queue: { QueueName: queueName, Attributes: { VisibilityTimeout: '2' } },
          },
          consumerOverrides: { heartbeatInterval: heartbeatEnabled ? 1 : undefined },
          removeHandlerOverride: async () => {
            consumer1IsProcessing = true
            await setTimeout(3100) // Wait to the visibility timeout to expire
            consumer1Counter++
            consumer1IsProcessing = false
            return { result: 'success' }
          },
        })
        await consumer1.start()

        const consumer2 = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl: consumer1.queueProps.url },
          removeHandlerOverride: () => {
            consumer2Counter++
            return Promise.resolve({ result: 'success' })
          },
        })
        const publisher = new SqsPermissionPublisher(diContainer.cradle, {
          locatorConfig: { queueUrl: consumer1.queueProps.url },
        })

        await publisher.publish({ id: '10', messageType: 'remove' })
        // wait for consumer1 to start processing to start second consumer
        await waitAndRetry(() => consumer1IsProcessing, 5, 5)
        await consumer2.start()

        // wait for both consumers to process message
        await waitAndRetry(() => consumer1Counter > 0 && consumer2Counter > 0, 100, 40)

        expect(consumer1Counter).toBe(1)
        expect(consumer2Counter).toBe(heartbeatEnabled ? 0 : 1)

        await Promise.all([consumer1.close(), consumer2.close()])
      },
      // This reduces flakiness in CI
      10000,
    )
  })

  describe('exponential backoff retry', () => {
    const queueName = 'myTestQueue_exponentialBackoffRetry'
    let diContainer: AwilixContainer<Dependencies>

    beforeEach(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('should use internal field and 1 base delay', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName: queueName },
        },
        removeHandlerOverride: () => {
          return Promise.resolve({ error: 'retryLater' })
        },
      })
      await consumer.start()

      const publisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: { queueUrl: consumer.queueProps.url },
      })
      await publisher.init()

      const sqsSpy = vi.spyOn(diContainer.cradle.sqsClient, 'send')
      await publisher.publish({
        id: '10',
        messageType: 'remove',
      })
      await publisher.publish({
        id: '20',
        messageType: 'remove',
        _internalRetryLaterCount: 1, // Note that publish will add 1 to this value, but it's fine for this test
      } as any)
      await publisher.publish({
        id: '30',
        messageType: 'remove',
        _internalRetryLaterCount: 10, // Note that publish will add 1 to this value, but it's fine for this test
      } as any)

      await waitAndRetry(
        () => {
          const sendMessageCommands = sqsSpy.mock.calls
            .map((call) => call[0].input)
            .filter((input) => 'MessageBody' in input)

          return sendMessageCommands.length === 6
        },
        5,
        100,
      )

      const sendMessageCommands = sqsSpy.mock.calls
        .map((call) => call[0].input)
        .filter((input) => 'MessageBody' in input)

      expect(sendMessageCommands).toHaveLength(6)
      expect(sendMessageCommands).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":0'),
          }),
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":2'),
          }),
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":11'),
          }),
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":1'),
            DelaySeconds: 1,
          }),
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":3'),
            DelaySeconds: 4,
          }),
          expect.objectContaining({
            MessageBody: expect.stringContaining('"_internalRetryLaterCount":12'),
            DelaySeconds: 2048,
          }),
        ]),
      )

      await publisher.close()
      await consumer.close()
    })
  })
})
