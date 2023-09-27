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
        messageType: 'add',
      })

      await waitAndRetry(() => {
        return logger.loggedMessages.length === 3
      })

      expect(logger.loggedMessages.length).toBe(3)
      expect(logger.loggedMessages[1]).toEqual({ messageType: 'add' })
      expect(logger.loggedMessages[2]).toEqual({ messageType: 'add' })
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
          return Promise.resolve(barrierCounter > 1)
        },
      })
      await newConsumer.start()

      publisher.publish({
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
      const newConsumer = new AmqpPermissionConsumerMultiSchema(diContainer.cradle, {
        addPreHandlerBarrier: (_msg) => {
          barrierCounter++
          if (barrierCounter === 1) {
            throw new Error()
          }
          return Promise.resolve(true)
        },
      })
      await newConsumer.start()

      publisher.publish({
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
        messageType: 'add',
      })
      publisher.publish({
        messageType: 'remove',
      })
      publisher.publish({
        messageType: 'remove',
      })

      await waitAndRetry(() => {
        return consumer.addCounter === 1 && consumer.removeCounter === 2
      })

      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(2)
    })

    it('Reconnects if connection is lost', async () => {
      await (await diContainer.cradle.amqpConnectionManager.getConnection()).close()
      publisher.publish({
        messageType: 'add',
      })

      await waitAndRetry(() => {
        publisher.publish({
          messageType: 'add',
        })

        return consumer.addCounter > 0
      })

      expect(consumer.addCounter > 0).toBe(true)
      await consumer.close()
    })
  })
})
