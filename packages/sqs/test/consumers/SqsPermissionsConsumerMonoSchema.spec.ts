import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver'
import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils'
import type { SqsPermissionPublisherMonoSchema } from '../publishers/SqsPermissionPublisherMonoSchema'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionConsumerMonoSchema } from './SqsPermissionConsumerMonoSchema'

const userIds = [100, 200, 300]
const perms: [string, ...string[]] = ['perm1', 'perm2']

async function retrievePermissions(userIds: number[]) {
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
}

describe('SqsPermissionsConsumerMonoSchema', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      await deleteQueue(sqsClient, 'existingQueue')
    })

    it('throws an error when invalid queue locator is passed', async () => {
      const newConsumer = new SqsPermissionConsumerMonoSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
        },
      })

      await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)

      await newConsumer.close()
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SqsPermissionConsumerMonoSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
        },
      })

      await newConsumer.init()
      expect(newConsumer.queueUrl).toBe(
        'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
      )

      await newConsumer.close()
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisherMonoSchema
    let sqsClient: SQSClient
    let consumer: SqsPermissionConsumerMonoSchema

    beforeEach(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer

      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      const command = new ReceiveMessageCommand({
        QueueUrl: diContainer.cradle.permissionPublisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()

      const fakeErrorResolver = diContainer.cradle
        .consumerErrorResolver as FakeConsumerErrorResolver
      fakeErrorResolver.clear()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
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
          id: 'abcd',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        await consumer.handlerSpy.waitForMessageWithId('abcd', 'consumed')
        const updatedUsersPermissions = await retrievePermissions(userIds)

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
          id: '123',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        // no users in the database, so message will go back to the queue
        await consumer.handlerSpy.waitForMessageWithId('123', 'retryLater')

        const usersFromDb = await retrievePermissions(userIds)
        expect(usersFromDb).toBeNull()

        userPermissionMap[100] = []
        userPermissionMap[200] = []
        userPermissionMap[300] = []

        await consumer.handlerSpy.waitForMessageWithId('123', 'consumed')
        const usersPermissions = await retrievePermissions(userIds)

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
          id: 'abc',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        // not all users are in the database, so message will go back to the queue
        await consumer.handlerSpy.waitForMessageWithId('abc', 'retryLater')

        const usersFromDb = await retrievePermissions(userIds)
        expect(usersFromDb).toBeNull()

        userPermissionMap[200] = []
        userPermissionMap[300] = []

        await consumer.handlerSpy.waitForMessageWithId('abc', 'consumed')
        const usersPermissions = await retrievePermissions(userIds)

        if (null === usersPermissions) {
          throw new Error('Users permissions unexpectedly null')
        }

        expect(usersPermissions).toBeDefined()
        expect(usersPermissions[0]).toHaveLength(2)
      })
    })
  })
})
