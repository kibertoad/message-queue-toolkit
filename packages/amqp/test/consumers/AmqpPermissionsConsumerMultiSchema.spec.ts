import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { waitAndRetry } from '../../../core/lib/utils/waitUtils'
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
        return logger.loggedMessages.length === 1
      })

      expect(logger.loggedMessages.length).toBe(1)
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
        return consumer.addCounter > 0 && consumer.removeCounter == 2
      })

      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(2)
    })
  })
})
