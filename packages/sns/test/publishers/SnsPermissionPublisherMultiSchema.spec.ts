import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { assertQueue, deleteQueue, purgeQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it, afterAll, beforeAll } from 'vitest'

import { subscribeToTopic } from '../../lib/sns/SnsSubscriber'
import { deserializeSNSMessage } from '../../lib/sns/snsMessageDeserializer'
import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils'
import { SnsSqsPermissionConsumer } from '../consumers/SnsSqsPermissionConsumer'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import type { PERMISSIONS_SUPPORTED_MESSAGES } from './SnsPermissionPublisherMultiSchema'
import { SnsPermissionPublisherMultiSchema } from './SnsPermissionPublisherMultiSchema'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]
const queueName = 'someQueue'

describe('SNSPermissionPublisherMultiSchema', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      snsClient = diContainer.cradle.snsClient
    })

    it('throws an error when invalid queue locator is passed', async () => {
      const newPublisher = new SnsPermissionPublisherMultiSchema(diContainer.cradle, {
        locatorConfig: {
          topicArn: 'dummy',
        },
      })

      await expect(() => newPublisher.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      const arn = await assertTopic(snsClient, {
        Name: 'existingTopic',
      })

      const newPublisher = new SnsPermissionPublisherMultiSchema(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
        },
      })

      await newPublisher.init()
      expect(newPublisher.topicArn).toEqual(arn)
      await deleteTopic(snsClient, 'existingTopic')
    })
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    let consumer: Consumer

    beforeAll(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      await purgeQueue(sqsClient, SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME)
    })

    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      await deleteQueue(sqsClient, queueName)
      await diContainer.cradle.permissionPublisher.init()
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      consumer?.stop()
      consumer?.stop({ abort: true })
      await purgeQueue(sqsClient, queueName)
    })

    it('publishes a message', async () => {
      const { permissionPublisherMultiSchema } = diContainer.cradle

      const message1 = {
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_SUPPORTED_MESSAGES

      const message2 = {
        userIds,
        messageType: 'remove',
        permissions: perms,
      } satisfies PERMISSIONS_SUPPORTED_MESSAGES

      const queueUrl = await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: queueName,
        },
        {
          Name: SnsPermissionPublisherMultiSchema.TOPIC_NAME,
        },
        {},
      )

      const receivedMessages: PERMISSIONS_SUPPORTED_MESSAGES[] = []
      consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: async (message: SQSMessage) => {
          if (message === null) {
            return
          }

          let deserializedMessage
          const result1 = deserializeSNSMessage(
            message as any,
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )

          if (result1.error) {
            const result2 = deserializeSNSMessage(
              message as any,
              PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
              new FakeConsumerErrorResolver(),
            )
            deserializedMessage = result2.result
          } else {
            deserializedMessage = result1.result
          }

          receivedMessages.push(deserializedMessage)
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      consumer.on('error', () => {})

      await permissionPublisherMultiSchema.publish(message1)
      await permissionPublisherMultiSchema.publish(message2)

      await waitAndRetry(() => {
        return receivedMessages.length === 2
      })

      expect(receivedMessages[0]).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })

      expect(receivedMessages[1]).toEqual({
        messageType: 'remove',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })
    }, 99999)
  })
})
