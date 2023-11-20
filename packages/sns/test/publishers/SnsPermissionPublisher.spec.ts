import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { assertQueue, deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { deserializeSNSMessage } from '../../lib/utils/snsMessageDeserializer'
import { subscribeToTopic } from '../../lib/utils/snsSubscriber'
import { assertTopic, deleteTopic, getTopicAttributes } from '../../lib/utils/snsUtils'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsPermissionPublisherMonoSchema } from './SnsPermissionPublisherMonoSchema'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]
const queueName = 'someQueue'

describe('SNSPermissionPublisher', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      snsClient = diContainer.cradle.snsClient
    })

    it('sets correct policy when policy fields are set', async () => {
      const newPublisher = new SnsPermissionPublisherMonoSchema(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'policy-topic',
          },
          queueUrlsWithSubscribePermissionsPrefix: 'dummy*',
        },
      })

      await newPublisher.init()

      const topic = await getTopicAttributes(snsClient, newPublisher.topicArn)

      expect(topic.result?.attributes?.Policy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-west-1:000000000000:policy-topic","Condition":{"StringLike":{"sns:Endpoint":"dummy*"}}}]}`,
      )
    })

    // FixMe https://github.com/localstack/localstack/issues/9306
    it.skip('throws an error when invalid queue locator is passed', async () => {
      const newPublisher = new SnsPermissionPublisherMonoSchema(diContainer.cradle, {
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

      const newPublisher = new SnsPermissionPublisherMonoSchema(diContainer.cradle, {
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

    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      await diContainer.cradle.permissionConsumer.close()

      await deleteQueue(sqsClient, queueName)
      await deleteTopic(snsClient, SnsPermissionPublisherMonoSchema.TOPIC_NAME)

      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]
    })

    afterEach(async () => {
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

      const { queueUrl } = await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: queueName,
        },
        {
          Name: SnsPermissionPublisherMonoSchema.TOPIC_NAME,
        },
        {
          updateAttributesIfExists: false,
        },
      )

      let receivedMessage: PERMISSIONS_MESSAGE_TYPE | null = null
      consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: async (message: SQSMessage) => {
          if (message === null) {
            return
          }
          const decodedMessage = deserializeSNSMessage(
            message as any,
            PERMISSIONS_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )
          receivedMessage = decodedMessage.result!
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      consumer.on('error', () => {})

      await permissionPublisher.publish(message)

      await waitAndRetry(() => {
        return receivedMessage !== null
      })

      expect(receivedMessage).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })

      consumer.stop()
    })
  })
})
