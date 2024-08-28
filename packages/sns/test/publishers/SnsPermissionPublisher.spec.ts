import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { InternalError } from '@lokalise/node-core'
import { waitAndRetry } from '@lokalise/node-core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { FakeConsumerErrorResolver, assertQueue, deleteQueue } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { deserializeSNSMessage } from '../../lib/utils/snsMessageDeserializer'
import { subscribeToTopic } from '../../lib/utils/snsSubscriber'
import { assertTopic, deleteTopic, getTopicAttributes } from '../../lib/utils/snsUtils'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsPermissionPublisher } from './SnsPermissionPublisher'

const queueName = 'someQueue'

describe('SnsPermissionPublisher', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies()
      snsClient = diContainer.cradle.snsClient
    })

    it('sets correct policy when policy fields are set', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'policy-topic',
          },
          queueUrlsWithSubscribePermissionsPrefix: 'dummy*',
        },
      })

      await newPublisher.init()

      const topic = await getTopicAttributes(snsClient, newPublisher.topicArnProp)

      expect(topic.result?.attributes?.Policy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-west-1:000000000000:policy-topic","Condition":{"StringLike":{"sns:Endpoint":"dummy*"}}}]}`,
      )
    })

    it('sets correct policy when two policy fields are set', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'policy-topic',
          },
          queueUrlsWithSubscribePermissionsPrefix: 'dummy*',
          allowedSourceOwner: '111111111111',
        },
      })

      await newPublisher.init()

      const topic = await getTopicAttributes(snsClient, newPublisher.topicArnProp)

      expect(topic.result?.attributes?.Policy).toBe(
        `{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"arn:aws:sns:eu-west-1:000000000000:policy-topic","Condition":{"StringEquals":{"AWS:SourceOwner":"111111111111"},"StringLike":{"sns:Endpoint":"dummy*"}}}]}`,
      )
    })

    // FixMe https://github.com/localstack/localstack/issues/9306
    it.skip('throws an error when invalid queue locator is passed', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
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

      const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
        },
      })

      await newPublisher.init()
      expect(newPublisher.topicArnProp).toEqual(arn)
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
      await deleteTopic(snsClient, SnsPermissionPublisher.TOPIC_NAME)
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

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
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
        {
          updateAttributesIfExists: false,
        },
      )

      let receivedMessage: unknown = null
      consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: (message: SQSMessage) => {
          if (message !== null) {
            const decodedMessage = deserializeSNSMessage(
              message as any,
              PERMISSIONS_ADD_MESSAGE_SCHEMA,
              new FakeConsumerErrorResolver(),
            )
            receivedMessage = decodedMessage.result!
          }
          return Promise.resolve()
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      consumer.on('error', () => {})

      await permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)

      expect(receivedMessage).toEqual({
        originalMessage: {
          ...message,
          _internalNumberOfRetries: 0,
          timestamp: expect.any(String),
        },
        parsedMessage: message,
      })

      consumer.stop()
    })

    it('publishes a message auto-filling internal properties', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

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
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
        {
          updateAttributesIfExists: false,
        },
      )

      let receivedMessage: unknown
      consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: (message: SQSMessage) => {
          if (message !== null) {
            const decodedMessage = deserializeSNSMessage(
              message as any,
              PERMISSIONS_ADD_MESSAGE_SCHEMA,
              new FakeConsumerErrorResolver(),
            )
            receivedMessage = decodedMessage.result!
          }
          return Promise.resolve()
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      consumer.on('error', () => {})

      await permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)

      expect(receivedMessage).toEqual({
        originalMessage: {
          ...message,
          timestamp: expect.any(String),
          _internalNumberOfRetries: 0,
        },
        parsedMessage: {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
        },
      })

      consumer.stop()
    })

    it('preserves message metadata in a publish error message', async () => {
      expect.assertions(1)
      const { permissionPublisher } = diContainer.cradle
      const permissions: [string, ...string[]] = ['perm']
      for (let i = 0; i < 5000; i++) {
        permissions.push('really-long-permissions-for-testing-excessively-large-payloads')
      }

      const message = {
        id: '1',
        messageType: 'add',
        permissions,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      await subscribeToTopic(
        sqsClient,
        snsClient,
        {
          QueueName: queueName,
        },
        {
          Name: SnsPermissionPublisher.TOPIC_NAME,
        },
        {
          updateAttributesIfExists: false,
        },
      )

      try {
        await permissionPublisher.publish(message)
      } catch (err) {
        expect((err as InternalError).details).toMatchInlineSnapshot(`
          {
            "messageType": "add",
            "publisher": "SnsPermissionPublisher",
            "topic": "arn:aws:sns:eu-west-1:000000000000:user_permissions_multi",
          }
        `)
      }
    })

    it('publish message with lazy loading', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle)

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await newPublisher.publish(message)

      const res = await newPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(res.message).toEqual(message)
    })
  })
})
