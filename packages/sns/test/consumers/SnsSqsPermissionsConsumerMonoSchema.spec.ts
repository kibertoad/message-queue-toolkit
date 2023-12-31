import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { FakeConsumerErrorResolver } from '@message-queue-toolkit/sqs'
import { assertQueue, deleteQueue, getQueueAttributes } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'
import z from 'zod'

import { assertTopic, deleteTopic, getTopicAttributes } from '../../lib/utils/snsUtils'
import type { SnsPermissionPublisherMonoSchema } from '../publishers/SnsPermissionPublisherMonoSchema'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsSqsPermissionConsumerMonoSchema } from './SnsSqsPermissionConsumerMonoSchema'

const userIds = [100, 200, 300]
const perms: [string, ...string[]] = ['perm1', 'perm2']

async function resolvePermissions(userIds: number[]) {
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

describe('SNS PermissionsConsumer', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies({}, false)
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      await deleteQueue(sqsClient, SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME)
    })

    it('sets correct policy when policy fields are set', async () => {
      const newConsumer = new SnsSqsPermissionConsumerMonoSchema(diContainer.cradle, {
        creationConfig: {
          queue: {
            QueueName: 'policy-queue',
          },
          topic: {
            Name: 'policy-topic',
          },
          topicArnsWithPublishPermissionsPrefix: 'dummy*',
          queueUrlsWithSubscribePermissionsPrefix: 'dummy*',
        },
      })
      await newConsumer.init()

      const queue = await getQueueAttributes(
        sqsClient,
        {
          queueUrl: newConsumer.queueUrl,
        },
        ['Policy'],
      )
      const topic = await getTopicAttributes(snsClient, newConsumer.topicArn)

      expect(queue.result?.attributes?.Policy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:eu-west-1:000000000000:policy-queue","Condition":{"ArnLike":{"aws:SourceArn":"dummy*"}}}]}`,
      )
      expect(topic.result?.attributes?.Policy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-west-1:000000000000:policy-topic","Condition":{"StringLike":{"sns:Endpoint":"dummy*"}}}]}`,
      )
    })

    // FixMe https://github.com/localstack/localstack/issues/9306
    it.skip('throws an error when invalid queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumerMonoSchema(diContainer.cradle, {
        locatorConfig: {
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          subscriptionArn: 'dummy',
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
    let consumer: SnsSqsPermissionConsumerMonoSchema
    let fakeResolver: FakeConsumerErrorResolver

    beforeEach(async () => {
      diContainer = await registerDependencies()
      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer
      fakeResolver = diContainer.cradle.consumerErrorResolver as FakeConsumerErrorResolver

      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle

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
          id: '1',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
        const updatedUsersPermissions = await resolvePermissions(userIds)

        if (null === updatedUsersPermissions) {
          throw new Error('Users permissions unexpectedly null')
        }

        expect(consumer.preHandlerBarrierCounter).toBe(3)
        expect(updatedUsersPermissions).toBeDefined()
        expect(updatedUsersPermissions[0]).toHaveLength(2)
      })

      it('Wait for users to be created and then create permissions', async () => {
        const users = Object.values(userPermissionMap)
        expect(users).toHaveLength(0)

        await publisher.publish({
          id: '2',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        await consumer.handlerSpy.waitForMessageWithId('2', 'retryLater')
        // no users in the database, so message will go back to the queue
        const usersFromDb = await resolvePermissions(userIds)
        expect(usersFromDb).toBeNull()

        userPermissionMap[100] = []
        userPermissionMap[200] = []
        userPermissionMap[300] = []

        await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
        const usersPermissions = await resolvePermissions(userIds)

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
          id: '3',
          messageType: 'add',
          userIds,
          permissions: perms,
        })

        await consumer.handlerSpy.waitForMessageWithId('3', 'retryLater')
        // not all users are in the database, so message will go back to the queue
        const usersFromDb = await resolvePermissions(userIds)
        expect(usersFromDb).toBeNull()

        userPermissionMap[200] = []
        userPermissionMap[300] = []

        await consumer.handlerSpy.waitForMessageWithId('3', 'consumed')
        const usersPermissions = await resolvePermissions(userIds)

        if (null === usersPermissions) {
          throw new Error('Users permissions unexpectedly null')
        }

        expect(usersPermissions).toBeDefined()
        expect(usersPermissions[0]).toHaveLength(2)
      })
    })

    describe('error handling', () => {
      it('Invalid message in the queue', async () => {
        // @ts-ignore
        publisher['messageSchema'] = z.any()
        await publisher.publish({
          id: 'abc',
          messageType: 'add',
          permissions: perms,
        } as any)

        const messageResult = await consumer.handlerSpy.waitForMessageWithId('abc')
        expect(messageResult.processingResult).toBe('invalid_message')

        expect(fakeResolver.handleErrorCallsCount).toBe(1)
      })

      it('Non-JSON message in the queue', async () => {
        // @ts-ignore
        publisher['messageSchema'] = z.any()
        await publisher.publish('dummy' as any)

        await waitAndRetry(() => {
          return fakeResolver.handleErrorCallsCount > 0
        })

        expect(fakeResolver.handleErrorCallsCount).toBe(1)
      })
    })
  })
})
