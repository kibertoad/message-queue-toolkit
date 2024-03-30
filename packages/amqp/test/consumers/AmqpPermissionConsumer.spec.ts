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

describe('AmqpPermissionConsumer', () => {
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

      expect(logger.loggedMessages.length).toBe(4)
      expect(logger.loggedMessages).toMatchInlineSnapshot(`
        [
          "Propagating new connection across 0 receivers",
          {
            "id": "1",
            "messageType": "add",
          },
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

  describe('prehandlers', () => {
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

    it('processes one prehandler', async () => {
      expect.assertions(1)

      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.prehandlerCount).toBe(1)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 1
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

    it('processes two prehandlers', async () => {
      expect.assertions(1)

      const newConsumer = new AmqpPermissionConsumer(diContainer.cradle, {
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.prehandlerCount).toBe(11)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 10
              : 10
            next({
              result: 'success',
            })
          },

          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 1
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
})
