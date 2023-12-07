import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import type { AmqpPermissionPublisherMultiSchema } from '../publishers/AmqpPermissionPublisherMultiSchema'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { AmqpPermissionConsumerMultiSchema } from './AmqpPermissionConsumerMultiSchema'

describe('PermissionsConsumerMultiSchema', () => {
  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisherMultiSchema
    beforeAll(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumerMultiSchema.close()
      publisher = diContainer.cradle.permissionPublisherMultiSchema
    })

    it('logs a message when logging is enabled', async () => {
      const newConsumer = new AmqpPermissionConsumerMultiSchema(diContainer.cradle, {
        logMessages: true,
      })
      await newConsumer.start()

      publisher.publish({
        id: '1',
        messageType: 'add',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

      expect(logger.loggedMessages.length).toBe(3)
      expect(logger.loggedMessages[1]).toEqual({
        id: '1',
        messageType: 'add',
      })
      expect(logger.loggedMessages[2]).toEqual({
        id: '1',
        messageType: 'add',
      })
    })
  })

  describe('preHandlerBarrier', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisherMultiSchema

    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG)
      await diContainer.cradle.permissionConsumerMultiSchema.close()
      publisher = diContainer.cradle.permissionPublisherMultiSchema
    })

    it('blocks first try', async () => {
      let barrierCounter = 0
      const newConsumer = new AmqpPermissionConsumerMultiSchema(diContainer.cradle, {
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
      const newConsumer = new AmqpPermissionConsumerMultiSchema(diContainer.cradle, {
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

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisherMultiSchema
    let consumer: AmqpPermissionConsumerMultiSchema

    beforeEach(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })

      publisher = diContainer.cradle.permissionPublisherMultiSchema
      consumer = diContainer.cradle.permissionConsumerMultiSchema
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
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
