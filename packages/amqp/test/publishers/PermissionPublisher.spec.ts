import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass, Lifetime } from 'awilix'
import { describe, beforeAll, beforeEach, afterAll, afterEach, expect, it } from 'vitest'

import { waitAndRetry } from '../../../core/lib/utils/waitUtils'
import { deserializeMessage } from '../../lib/messageDeserializer'
import { PermissionConsumer } from '../consumers/PermissionConsumer'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakeConsumer } from '../fakes/FakeConsumer'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { PermissionPublisher } from './PermissionPublisher'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

describe('PermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
        permissionConsumer: asClass(FakeConsumer, {
          lifetime: Lifetime.SINGLETON,
          asyncInit: 'consume',
          asyncDispose: 'close',
          asyncDisposePriority: 10,
        }),
      })
    })

    beforeEach(async () => {
      channel = await diContainer.cradle.amqpConnection.createChannel()
    })

    afterEach(async () => {
      await channel.deleteQueue(PermissionConsumer.QUEUE_NAME)
      await channel.close()
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      let receivedMessage: PERMISSIONS_MESSAGE_TYPE | null = null
      await channel.consume(PermissionPublisher.QUEUE_NAME, (message) => {
        if (message === null) {
          return
        }
        const decodedMessage = deserializeMessage(
          message,
          PERMISSIONS_MESSAGE_SCHEMA,
          new FakeConsumerErrorResolver(),
        )
        receivedMessage = decodedMessage.result!
      })

      permissionPublisher.publish(message)

      await waitAndRetry(() => {
        return receivedMessage !== null
      })

      expect(receivedMessage).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })
    })
  })
})
