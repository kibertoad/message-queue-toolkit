import {
  type ProcessedMessageMetadata,
  objectToBuffer,
  waitAndRetry,
} from '@message-queue-toolkit/core'
import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { ZodError } from 'zod'

import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import type { AmqpPermissionPublisher } from '../publishers/AmqpPermissionPublisher'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { SINGLETON_CONFIG, registerDependencies } from '../utils/testContext'

import { AmqpPermissionConsumer } from './AmqpPermissionConsumer'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'

describe('AmqpPermissionConsumer', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>

    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('updateAttributesIfExists is not supported', async () => {
      const consumer = new AmqpPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          updateAttributesIfExists: true,
          queueName: 'dummy',
          queueOptions: { durable: true, autoDelete: false },
        },
      })

      await expect(consumer.init()).rejects.toThrow(
        'updateAttributesIfExists parameter is not currently supported by the AMQP adapter',
      )
    })

    it('deadLetterQueue is not supported', async () => {
      const consumer = new AmqpPermissionConsumer(diContainer.cradle, {
        deadLetterQueue: {
          creationConfig: {
            queueName: 'dummy-dlq',
            queueOptions: { durable: true, autoDelete: false },
          },
        },
      })

      await expect(consumer.init()).rejects.toThrow(
        'deadLetterQueue parameter is not currently supported by the Amqp adapter',
      )
    })
  })

  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher
    beforeAll(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    it('logs a message when logging is enabled', async () => {
      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        logMessages: true,
      })
      await newConsumer.start()

      publisher.publish({
        id: '1',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      await newConsumer.close()

      expect(logger.loggedMessages.length).toBe(6)
      expect(logger.loggedMessages).toMatchObject([
        'Propagating new connection across 0 receivers',
        {
          id: '1',
          messageType: 'add',
        },
        'timestamp not defined, adding it automatically',
        expect.any(Object),
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
    })
  })

  describe('metrics', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher
    beforeAll(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    it('registers metrics if metrics manager is provided', async () => {
      const messagesRegisteredInMetrics: ProcessedMessageMetadata[] = []
      const newConsumer = new AmqpPermissionConsumer({
        ...diContainer.cradle,
        messageMetricsManager: {
          registerProcessedMessage(metadata: ProcessedMessageMetadata): void {
            messagesRegisteredInMetrics.push(metadata)
          },
        },
      })
      await newConsumer.start()

      publisher.publish({
        id: '1',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      await newConsumer.close()

      expect(messagesRegisteredInMetrics).toStrictEqual([
        {
          messageId: '1',
          messageType: 'add',
          messageDeduplicationId: undefined,
          processingResult: { status: 'consumed' },
          queueName: AmqpPermissionConsumer.QUEUE_NAME,
          messageTimestamp: expect.any(Number),
          messageProcessingStartTimestamp: expect.any(Number),
          messageProcessingEndTimestamp: expect.any(Number),
          message: expect.objectContaining({
            id: '1',
            messageType: 'add',
          }),
        },
      ])
    })
  })

  describe('preHandlerBarrier', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher

    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG)
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    it('blocks first try', async () => {
      let barrierCounter = 0
      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          if (barrierCounter < 2) {
            return Promise.resolve({
              isPassing: false,
            })
          }

          return Promise.resolve({
            isPassing: true,
            output: barrierCounter,
          })
        },
      })
      await newConsumer.start()

      publisher.publish({
        id: '3',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('3', 'retryLater')
      await newConsumer.handlerSpy.waitForMessageWithId('3', 'consumed')

      await newConsumer.close()

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
    })

    it('throws an error on first try', async () => {
      let barrierCounter = 0
      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          if (barrierCounter === 1) {
            throw new Error()
          }
          return Promise.resolve({
            isPassing: true,
            output: barrierCounter,
          })
        },
      })
      await newConsumer.start()

      publisher.publish({
        id: '4',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('4', 'retryLater')
      await newConsumer.handlerSpy.waitForMessageWithId('4', 'consumed')

      await newConsumer.close()

      expect(newConsumer.addCounter).toBe(1)
      expect(barrierCounter).toBe(2)
    })
  })

  describe('preHandlers', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher
    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, undefined, false)
      publisher = diContainer.cradle.permissionPublisher
      await publisher.init()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('processes one preHandler', async () => {
      expect.assertions(1)

      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: (_message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.preHandlerCount).toBe(1)
          return Promise.resolve({
            result: 'success',
          })
        },
        removePreHandlers: [
          (_message, _context, preHandlerOutput, next) => {
            preHandlerOutput.preHandlerCount = preHandlerOutput.preHandlerCount
              ? preHandlerOutput.preHandlerCount + 1
              : 1
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })

    it('processes two preHandlers', async () => {
      expect.assertions(1)

      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: (_message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.preHandlerCount).toBe(11)
          return Promise.resolve({
            result: 'success',
          })
        },
        removePreHandlers: [
          (_message, _context, preHandlerOutput, next) => {
            preHandlerOutput.preHandlerCount = preHandlerOutput.preHandlerCount
              ? preHandlerOutput.preHandlerCount + 10
              : 10
            next({
              result: 'success',
            })
          },

          (_message, _context, preHandlerOutput, next) => {
            preHandlerOutput.preHandlerCount = preHandlerOutput.preHandlerCount
              ? preHandlerOutput.preHandlerCount + 1
              : 1
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let consumer: AmqpPermissionConsumer
    let publisher: AmqpPermissionPublisher
    let channel: Channel
    let consumerErrorResolver: FakeConsumerErrorResolver

    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })

      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer
      consumerErrorResolver = diContainer.cradle.consumerErrorResolver as FakeConsumerErrorResolver
      channel = await (
        await diContainer.cradle.amqpConnectionManager.getConnection()
      ).createChannel()
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle

      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('Invalid message in the queue', async () => {
      const invalidMessage = {
        id: 1, // invalid type
        messageType: 'add',
      }

      expect(() =>
        // @ts-ignore
        publisher.publish(invalidMessage),
      ).toThrowErrorMatchingInlineSnapshot(
        `
        [ZodError: [
          {
            "code": "invalid_type",
            "expected": "string",
            "received": "number",
            "path": [
              "id"
            ],
            "message": "Expected string, received number"
          }
        ]]
      `,
      )

      channel.sendToQueue(AmqpPermissionConsumer.QUEUE_NAME, objectToBuffer(invalidMessage))

      await waitAndRetry(() => consumerErrorResolver.errors.length > 0)

      expect(consumerErrorResolver.errors).toHaveLength(1)
      expect(consumerErrorResolver.errors[0] instanceof ZodError).toBe(true)
    })

    it('message with invalid message type', async () => {
      const errorReporterSpy = vi.spyOn(diContainer.cradle.errorReporter, 'report')
      channel.sendToQueue(
        AmqpPermissionConsumer.QUEUE_NAME,
        objectToBuffer({
          id: '1',
          messageType: 'bad',
        }),
      )

      await waitAndRetry(() => errorReporterSpy.mock.calls.length > 0)

      expect(errorReporterSpy.mock.calls).toHaveLength(1)
      expect(errorReporterSpy.mock.calls[0][0].error).toMatchObject({
        message: 'Unsupported message type: bad',
      })
    })

    it('message in the queue is not JSON', async () => {
      channel.sendToQueue(AmqpPermissionConsumer.QUEUE_NAME, Buffer.from('not a JSON'))

      await waitAndRetry(() => consumerErrorResolver.errors.length > 0)

      expect(consumerErrorResolver.errors.length).toBeGreaterThan(0)
      expect((consumerErrorResolver.errors[0] as Error).message).toContain('Unexpected token')
    })

    it('Processes messages', async () => {
      publisher.publish({
        id: '10',
        messageType: 'add',
      })
      publisher.publish({
        id: '20',
        messageType: 'remove',
      })
      publisher.publish({
        id: '30',
        messageType: 'remove',
      })

      await consumer.handlerSpy.waitForMessageWithId('10', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('20', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('30', 'consumed')

      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(2)
    })

    it('Reconnects if connection is lost', async () => {
      await (await diContainer.cradle.amqpConnectionManager.getConnection()).close()
      publisher.publish({
        id: '100',
        messageType: 'add',
      })

      await waitAndRetry(() => {
        publisher.publish({
          id: '200',
          messageType: 'add',
        })

        return consumer.addCounter > 0
      })

      await consumer.handlerSpy.waitForMessageWithId('200', 'consumed')

      expect(consumer.addCounter > 0).toBe(true)
      await consumer.close()
    })
  })

  describe('messages stuck on retryLater', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher
    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, undefined, false)
      publisher = diContainer.cradle.permissionPublisher
      await publisher.init()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('stuck on barrier', async () => {
      let counter = 0
      const consumer = new AmqpPermissionConsumer(diContainer.cradle, {
        addPreHandlerBarrier: () => {
          counter++
          return Promise.resolve({ isPassing: false })
        },
        maxRetryDuration: 3,
      })
      await consumer.start()

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: '1',
        messageType: 'add',
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
      }
      publisher.publish(message)

      const jobSpy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(jobSpy.message).toEqual(message)
      expect(counter).toBeGreaterThan(2)

      await consumer.close()
    })

    it('stuck on handler', async () => {
      let counter = 0
      const consumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
        maxRetryDuration: 3,
      })
      await consumer.start()

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
      }
      publisher.publish(message)

      const jobSpy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(jobSpy.message).toEqual(message)
      expect(counter).toBeGreaterThan(2)

      await consumer.close()
    })
  })
})
