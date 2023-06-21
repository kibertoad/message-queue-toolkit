import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { buildQueueMessage } from '../../lib/utils/queueUtils'
import { waitAndRetry } from '../../lib/utils/waitUtils'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { PermissionConsumer } from './PermissionConsumer'
import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'

const userIds = [100, 200, 300]
const perms: [string, ...string[]] = ['perm1', 'perm2']

async function waitForPermissions(userIds: number[]) {
  return await waitAndRetry(
    async () => {
      const usersPerms = userIds.reduce((acc, userId) => {
        if (userPermissionMap[userId]) {
          acc.push(userPermissionMap[userId])
        }
        return acc
      }, [] as string[][])

      if (usersPerms && usersPerms.length !== userIds.length) {
        return null
      }

      for (const userPerms of usersPerms)
        if (userPerms.length !== perms.length) {
          return null
        }

      return usersPerms
    },
    500,
    5,
  )
}

describe('PermissionsConsumer', () => {
  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })

      channel = await diContainer.cradle.amqpConnection.createChannel()
      await diContainer.cradle.permissionConsumer.consume()
    })

    afterEach(async () => {
      await channel.deleteQueue(PermissionConsumer.QUEUE_NAME)
      await channel.close()
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('Creates permissions', async () => {
      const users = Object.values(userPermissionMap)
      expect(users).toHaveLength(0)

      userPermissionMap[100] = []
      userPermissionMap[200] = []
      userPermissionMap[300] = []

      void channel.sendToQueue(
        PermissionConsumer.QUEUE_NAME,
        buildQueueMessage({
          messageType: 'add',
          userIds,
          permissions: perms,
        } satisfies PERMISSIONS_MESSAGE_TYPE),
      )

      const updatedUsersPermissions = await waitForPermissions(userIds)

      if (null === updatedUsersPermissions) {
        throw new Error('Users permissions unexpectedly null')
      }

      expect(updatedUsersPermissions).toBeDefined()
      expect(updatedUsersPermissions[0]).toHaveLength(2)
    })

    it('Wait for users to be created and then create permissions', async () => {
      const users = Object.values(userPermissionMap)
      expect(users).toHaveLength(0)

      channel.sendToQueue(
        PermissionConsumer.QUEUE_NAME,
        buildQueueMessage({
          userIds,
          messageType: 'add',
          permissions: perms,
        } satisfies PERMISSIONS_MESSAGE_TYPE),
      )

      // no users in the database, so message will go back to the queue
      const usersFromDb = await waitForPermissions(userIds)
      expect(usersFromDb).toBeNull()

      userPermissionMap[100] = []
      userPermissionMap[200] = []
      userPermissionMap[300] = []

      const usersPermissions = await waitForPermissions(userIds)

      if (null === usersPermissions) {
        throw new Error('Users permissions unexpectedly null')
      }

      expect(usersPermissions).toBeDefined()
      expect(usersPermissions[0]).toHaveLength(2)
    })

    it('Not all users exist, no permissions were created', async () => {
      const users = Object.values(userPermissionMap)
      expect(users).toHaveLength(0)

      userPermissionMap[100] = []

      channel.sendToQueue(
        PermissionConsumer.QUEUE_NAME,
        buildQueueMessage({
          userIds,
          messageType: 'add',
          permissions: perms,
        } satisfies PERMISSIONS_MESSAGE_TYPE),
      )

      // not all users are in the database, so message will go back to the queue
      const usersFromDb = await waitForPermissions(userIds)
      expect(usersFromDb).toBeNull()

      userPermissionMap[200] = []
      userPermissionMap[300] = []

      const usersPermissions = await waitForPermissions(userIds)

      if (null === usersPermissions) {
        throw new Error('Users permissions unexpectedly null')
      }

      expect(usersPermissions).toBeDefined()
      expect(usersPermissions[0]).toHaveLength(2)
    })

    it('Invalid message in the queue', async () => {
      const { consumerErrorResolver } = diContainer.cradle

      channel.sendToQueue(
        PermissionConsumer.QUEUE_NAME,
        buildQueueMessage({
          messageType: 'add',
          permissions: perms,
        } as PERMISSIONS_MESSAGE_TYPE),
      )

      const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
      await waitAndRetry(() => fakeResolver.handleErrorCallsCount, 500, 5)

      expect(fakeResolver.handleErrorCallsCount).toBe(1)
    })

    it('Non-JSON message in the queue', async () => {
      const { consumerErrorResolver } = diContainer.cradle

      channel.sendToQueue(PermissionConsumer.QUEUE_NAME, Buffer.from('dummy'))

      const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
      await waitAndRetry(() => fakeResolver.handleErrorCallsCount, 500, 5)

      expect(fakeResolver.handleErrorCallsCount).toBe(1)
    })
  })
})
