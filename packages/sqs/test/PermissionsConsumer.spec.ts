import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import { PermissionConsumer } from './PermissionConsumer'
import type { PermissionPublisher } from './PermissionPublisher'
import { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver'
import { userPermissionMap } from './repositories/PermissionRepository'
import { deleteQueue, purgeQueue } from './utils/sqsUtils'
import { registerDependencies, SINGLETON_CONFIG } from './utils/testContext'
import type { Dependencies } from './utils/testContext'

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
    let publisher: PermissionPublisher
    let sqsClient: SQSClient
    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
      await deleteQueue(sqsClient, PermissionConsumer.QUEUE_NAME)

      await diContainer.cradle.permissionConsumer.consume()
      await diContainer.cradle.permissionPublisher.init()

      const command = new ReceiveMessageCommand({
        QueueUrl: diContainer.cradle.permissionPublisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle

      await purgeQueue(sqsClient, PermissionConsumer.QUEUE_NAME)
      await diContainer.cradle.permissionConsumer.close()
      await diContainer.cradle.permissionConsumer.close(true)
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    describe('happy path', () => {
      it('Creates permissions', async () => {
        const users = Object.values(userPermissionMap)
        expect(users).toHaveLength(0)

        userPermissionMap[100] = []
        userPermissionMap[200] = []
        userPermissionMap[300] = []

        await publisher.publish({
          messageType: 'add',
          userIds,
          permissions: perms,
        })

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

        await publisher.publish({
          messageType: 'add',
          userIds,
          permissions: perms,
        })

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

      it('Not all users exist, no permissions were created initially', async () => {
        const users = Object.values(userPermissionMap)
        expect(users).toHaveLength(0)

        userPermissionMap[100] = []

        await publisher.publish({
          messageType: 'add',
          userIds,
          permissions: perms,
        })

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
    })

    describe('error handling', () => {
      it('Invalid message in the queue', async () => {
        const { consumerErrorResolver } = diContainer.cradle

        await publisher.publish({
          messageType: 'add',
          permissions: perms,
        } as any)

        const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
        await waitAndRetry(() => fakeResolver.handleErrorCallsCount, 500, 5)

        expect(fakeResolver.handleErrorCallsCount).toBe(1)
      })

      it('Non-JSON message in the queue', async () => {
        const { consumerErrorResolver } = diContainer.cradle

        await publisher.publish('dummy' as any)
        await publisher.publish('dummy2' as any)
        await publisher.publish('dummy3' as any)

        const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
        const errorCount = await waitAndRetry(
          () => {
            return fakeResolver.handleErrorCallsCount
          },
          500,
          5,
        )

        expect(errorCount > 0 && errorCount < 4).toBe(true)
      }, 9999999)
    })
  })
})
