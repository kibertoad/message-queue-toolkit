import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction, Lifetime } from 'awilix'
import { describe, beforeAll, beforeEach, afterAll, afterEach, expect, it } from 'vitest'

import { waitAndRetry } from '../../../core/lib/utils/waitUtils'
import { deserializeAmqpMessage } from '../../lib/amqpMessageDeserializer'
import { AmqpPermissionConsumer } from '../consumers/AmqpPermissionConsumer'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakeConsumer } from '../fakes/FakeConsumer'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { AmqpPermissionPublisher } from './AmqpPermissionPublisher'
import type { AmqpPermissionPublisherMultiSchema } from './AmqpPermissionPublisherMultiSchema'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

function checkPermissions(userIds: number[]) {
  const usersPerms = userIds.reduce((acc, userId) => {
    if (userPermissionMap[userId]) {
      acc.push(userPermissionMap[userId])
    }
    return acc
  }, [] as string[][])

  if (usersPerms.length > userIds.length) {
    return usersPerms.slice(0, userIds.length - 1)
  }

  if (usersPerms && usersPerms.length !== userIds.length) {
    return null
  }

  for (const userPerms of usersPerms)
    if (userPerms.length !== perms.length) {
      return null
    }

  return usersPerms
}

describe('PermissionPublisher', () => {
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
      const message = {
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      publisher.publish(message)

      await waitAndRetry(() => {
        return logger.loggedMessages.length === 2
      })

      expect(logger.loggedMessages[1]).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })
    })
  })

  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
        permissionConsumer: asClass(FakeConsumer, {
          lifetime: Lifetime.SINGLETON,
          asyncInit: 'start',
          asyncDispose: 'close',
          asyncDisposePriority: 10,
        }),
      })
    })

    beforeEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
    })

    afterEach(async () => {
      await channel.deleteQueue(AmqpPermissionConsumer.QUEUE_NAME)
      await channel.close()
    })

    it('throws an error when invalid queue locator is passed', async () => {
      await channel.deleteQueue(AmqpPermissionConsumer.QUEUE_NAME)
      const newPublisher = new AmqpPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueName: AmqpPermissionPublisher.QUEUE_NAME,
        },
      })

      await expect(() => newPublisher.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await channel.assertQueue(AmqpPermissionPublisher.QUEUE_NAME)

      const newPublisher = new AmqpPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueName: AmqpPermissionPublisher.QUEUE_NAME,
        },
      })

      await expect(newPublisher.init()).resolves.toBeUndefined()
    })
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
    })

    beforeEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
    })

    afterEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
      await channel.deleteQueue(AmqpPermissionConsumer.QUEUE_NAME)
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
      await channel.consume(AmqpPermissionPublisher.QUEUE_NAME, (message) => {
        if (message === null) {
          return
        }
        const decodedMessage = deserializeAmqpMessage(
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

    it('reconnects on lost connection', async () => {
      const users = Object.values(userPermissionMap)
      expect(users).toHaveLength(0)

      userPermissionMap[100] = []
      userPermissionMap[200] = []
      userPermissionMap[300] = []

      const { permissionPublisher, permissionConsumer } = diContainer.cradle
      await permissionConsumer.start()

      const message = {
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      await diContainer.cradle.amqpConnectionManager.close()

      // wait till we are done reconnecting
      await waitAndRetry(() => {
        return diContainer.cradle.amqpConnectionManager.getConnectionSync()
      })

      const updatedUsersPermissions = await waitAndRetry(
        () => {
          permissionPublisher.publish(message)

          return checkPermissions(userIds)
        },
        100,
        20,
      )

      if (null === updatedUsersPermissions) {
        throw new Error('Users permissions unexpectedly null')
      }

      expect(updatedUsersPermissions).toBeDefined()
      expect(updatedUsersPermissions[0]).toHaveLength(2)
    })
  })
})
