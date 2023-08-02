import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import { assertQueue, deleteQueue, purgeQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, afterAll, beforeAll } from 'vitest'

import { assertTopic, deleteSubscription, deleteTopic } from '../../lib/utils/snsUtils'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import type { SnsPermissionPublisherMonoSchema } from '../publishers/SnsPermissionPublisherMonoSchema'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsSqsPermissionConsumerMonoSchema } from './SnsSqsPermissionConsumerMonoSchema'

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

describe('SNS PermissionsConsumer', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      await deleteQueue(sqsClient, SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME)
    })

    it('throws an error when invalid queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumerMonoSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          topicArn: 'dummy',
        },
      })

      await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const arn = await assertTopic(snsClient, {
        Name: 'existingTopic',
      })

      const newConsumer = new SnsSqsPermissionConsumerMonoSchema(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
        },
      })

      await newConsumer.init()
      expect(newConsumer.queueUrl).toBe(
        'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
      )
      expect(newConsumer.topicArn).toEqual(arn)
      expect(newConsumer.subscriptionArn).toBe(
        'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
      )
      await deleteTopic(snsClient, 'existingTopic')
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisherMonoSchema
    let sqsClient: SQSClient
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      publisher = diContainer.cradle.permissionPublisher
      await purgeQueue(sqsClient, SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME)
    })

    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      await deleteTopic(snsClient, SnsSqsPermissionConsumerMonoSchema.SUBSCRIBED_TOPIC_NAME)
      await deleteQueue(sqsClient, SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME)
      await diContainer.cradle.permissionConsumer.start()
      await diContainer.cradle.permissionPublisher.init()

      const queueUrl = await assertQueue(sqsClient, {
        QueueName: SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME,
      })
      const command = new ReceiveMessageCommand({
        QueueUrl: queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()

      const fakeErrorResolver = diContainer.cradle
        .consumerErrorResolver as FakeConsumerErrorResolver
      fakeErrorResolver.clear()
    })

    afterAll(async () => {
      const { awilixManager, permissionConsumer } = diContainer.cradle

      await deleteSubscription(snsClient, permissionConsumer.subscriptionArn)

      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      await purgeQueue(sqsClient, SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME)
      await diContainer.cradle.permissionConsumer.close()
      await diContainer.cradle.permissionConsumer.close(true)
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

        const fakeResolver = consumerErrorResolver as FakeConsumerErrorResolver
        const errorCount = await waitAndRetry(
          () => {
            return fakeResolver.handleErrorCallsCount
          },
          500,
          5,
        )

        expect(errorCount).toBe(1)
      })
    })
  })
})
