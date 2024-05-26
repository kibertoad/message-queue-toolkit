import { objectToBuffer, waitAndRetry } from '@message-queue-toolkit/core'
import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'
import { ZodError } from 'zod'

import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import type { AmqpPermissionPublisher } from '../publishers/AmqpPermissionPublisher'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { AmqpPermissionConsumer } from './AmqpPermissionConsumer'
import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'

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

      expect(logger.loggedMessages.length).toBe(5)
      expect(logger.loggedMessages).toEqual([
        'Propagating new connection across 0 receivers',
        'timestamp not defined, adding it automatically',
        {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
        },
        {
          id: '1',
          messageType: 'add',
        },
        {
          messageId: '1',
          processingResult: 'consumed',
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
        removeHandlerOverride: async (message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.preHandlerCount).toBe(1)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, preHandlerOutput, next) => {
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
        removeHandlerOverride: async (message, _context, preHandlerOutputs) => {
          expect(preHandlerOutputs.preHandlerOutput.preHandlerCount).toBe(11)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, preHandlerOutput, next) => {
            preHandlerOutput.preHandlerCount = preHandlerOutput.preHandlerCount
              ? preHandlerOutput.preHandlerCount + 10
              : 10
            next({
              result: 'success',
            })
          },

          (message, context, preHandlerOutput, next) => {
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
      channel.sendToQueue(
        AmqpPermissionConsumer.QUEUE_NAME,
        objectToBuffer({
          id: 1, // invalid type
          messageType: 'add',
        }),
      )

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
        addPreHandlerBarrier: async () => {
          counter++
          return { isPassing: false }
        },
        maxRetryDuration: 3,
      })
      await consumer.start()

      const message: PERMISSIONS_MESSAGE_TYPE = {
        id: '1',
        messageType: 'add',
        userIds: [1],
        permissions: ['100'],
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
      }
      publisher.publish(message)

      const jobSpy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(jobSpy.message).toEqual(message)
      expect(counter).toBeGreaterThan(2)
    })

    it('stuck on handler', async () => {
      let counter = 0
      const consumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: async () => {
          counter++
          return { error: 'retryLater' }
        },
        maxRetryDuration: 3,
      })
      await consumer.start()

      const message: PERMISSIONS_MESSAGE_TYPE = {
        id: '1',
        messageType: 'remove',
        userIds: [1],
        permissions: ['100'],
        timestamp: new Date(new Date().getTime() - 2 * 1000).toISOString(),
      }
      publisher.publish(message)

      const jobSpy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(jobSpy.message).toEqual(message)
      expect(counter).toBeGreaterThan(2)
    })
  })
})
