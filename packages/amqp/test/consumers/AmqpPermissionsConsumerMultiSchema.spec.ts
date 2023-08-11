import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import type { AmqpPermissionPublisherMultiSchema } from '../publishers/AmqpPermissionPublisherMultiSchema'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import type { AmqpPermissionConsumerMultiSchema } from './AmqpPermissionConsumerMultiSchema'

describe('PermissionsConsumerMultiSchema', () => {
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
      consumer.resetCounters()
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

      expect(consumer.addBarrierCounter).toBe(3)
      expect(consumer.addCounter).toBe(1)
      expect(consumer.removeCounter).toBe(2)
    })
  })
})
