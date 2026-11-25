import { ListTagsForResourceCommand, PublishCommand, type SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { InternalError } from '@lokalise/node-core'
import { waitAndRetry } from '@lokalise/node-core'
import type { SQSMessage } from '@message-queue-toolkit/sqs'
import { FakeConsumerErrorResolver } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import z from 'zod/v4'
import { AbstractSnsPublisher } from '../../lib/sns/AbstractSnsPublisher.ts'
import type { SNSDependencies } from '../../lib/sns/AbstractSnsService.ts'
import { deserializeSNSMessage } from '../../lib/utils/snsMessageDeserializer.ts'
import { subscribeToTopic } from '../../lib/utils/snsSubscriber.ts'
import { assertTopic, getTopicAttributes } from '../../lib/utils/snsUtils.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SnsPermissionPublisher } from './SnsPermissionPublisher.ts'

// Every other SNS test schema parses to a structurally identical object, so a schema whose output
// differs from its input is needed to cover what the publisher actually puts on the wire.
const SCHEMA_DEFAULTS_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('schemaDefaults'),
  timestamp: z.string().optional(),
  source: z.string().default('default-source'),
  attempt: z.coerce.number().optional(),
})

class SnsSchemaDefaultsPublisher extends AbstractSnsPublisher<
  z.input<typeof SCHEMA_DEFAULTS_MESSAGE_SCHEMA>
> {
  constructor(dependencies: SNSDependencies) {
    super(dependencies, {
      creationConfig: { topic: { Name: 'schema_defaults' } },
      deletionConfig: { deleteIfExists: false },
      messageSchemas: [SCHEMA_DEFAULTS_MESSAGE_SCHEMA],
      handlerSpy: true,
      messageTypeResolver: { messageTypePath: 'messageType' },
    })
  }
}

describe('SnsPermissionPublisher', () => {
  describe('init', () => {
    const topicNome = 'existingTopic'

    let diContainer: AwilixContainer<Dependencies>
    let snsClient: SNSClient
    let stsClient: STSClient
    let testAdmin: TestAwsResourceAdmin

    beforeAll(async () => {
      diContainer = await registerDependencies()
      snsClient = diContainer.cradle.snsClient
      stsClient = diContainer.cradle.stsClient
      testAdmin = diContainer.cradle.testAdmin
    })

    beforeEach(async () => {
      await testAdmin.deleteTopics(topicNome)
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
      const arn = await testAdmin.createTopic(topicNome)

      const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
        },
      })

      await newPublisher.init()
      expect(newPublisher.topicArnProp).toEqual(arn)
    })

    describe('tags', () => {
      const getTags = (arn: string) =>
        snsClient.send(new ListTagsForResourceCommand({ ResourceArn: arn }))

      it('updates existing topic tags when update is forced', async () => {
        const initialTags = [
          { Key: 'project', Value: 'some-project' },
          { Key: 'service', Value: 'some-service' },
          { Key: 'leftover', Value: 'some-leftover' },
        ]
        const newTags = [
          { Key: 'project', Value: 'some-project' },
          { Key: 'service', Value: 'changed-service' },
          { Key: 'cc', Value: 'some-cc' },
        ]

        const arn = await assertTopic(snsClient, stsClient, {
          Name: topicNome,
          Tags: initialTags,
        })
        const preTags = await getTags(arn)
        expect(preTags.Tags).toEqual(initialTags)

        const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            topic: { Name: topicNome, Tags: newTags },
            forceTagUpdate: true,
          },
        })

        const snsSpy = vi.spyOn(snsClient, 'send')
        await newPublisher.init()

        const updateCall = snsSpy.mock.calls.find((entry) => {
          return entry[0].constructor.name === 'TagResourceCommand'
        })
        expect(updateCall).toBeDefined()

        const postTags = await getTags(arn)
        const tags = postTags.Tags
        expect(tags).toHaveLength(4)
        expect(postTags.Tags).toEqual(
          expect.arrayContaining([...newTags, { Key: 'leftover', Value: 'some-leftover' }]),
        )
      })

      it('should throw error if tags are different and force tag update is not true', async () => {
        const initialTags = [
          { Key: 'project', Value: 'some-project' },
          { Key: 'service', Value: 'some-service' },
          { Key: 'leftover', Value: 'some-leftover' },
        ]
        const arn = await assertTopic(snsClient, stsClient, {
          Name: topicNome,
          Tags: initialTags,
        })
        const preTags = await getTags(arn)
        expect(preTags.Tags).toEqual(initialTags)

        const newPublisher = new SnsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            topic: { Name: topicNome, Tags: [{ Key: 'example', Value: 'should fail' }] },
          },
        })

        await expect(newPublisher.init()).rejects.toThrowError(
          `${topicNome} - Invalid parameter: Tags Reason: Topic already exists with different tags`,
        )
      })
    })
  })

  describe('publish', () => {
    const queueName = 'someQueue'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    let stsClient: STSClient
    let testAdmin: TestAwsResourceAdmin

    let consumer: Consumer

    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
      stsClient = diContainer.cradle.stsClient
      testAdmin = diContainer.cradle.testAdmin
      await diContainer.cradle.permissionConsumer.close()

      await testAdmin.deleteQueues(queueName)
      await testAdmin.deleteTopics(SnsPermissionPublisher.TOPIC_NAME)
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

      const { queueUrl } = await testAdmin.createQueue(queueName)

      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
          return Promise.resolve(message)
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
          _internalRetryLaterCount: 0,
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

      const { queueUrl } = await testAdmin.createQueue(queueName)

      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
          return Promise.resolve(message)
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
          _internalRetryLaterCount: 0,
        },
        parsedMessage: {
          id: '1',
          messageType: 'add',
          timestamp: expect.any(String),
        },
      })

      consumer.stop()
    })

    it('publishes the schema-parsed message, dropping fields absent from the schema', async () => {
      const { permissionPublisher } = diContainer.cradle
      await permissionPublisher.init()

      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        notInSchema: 'should-not-be-published',
      } as unknown as PERMISSIONS_ADD_MESSAGE_TYPE

      const snsSpy = vi.spyOn(snsClient, 'send')

      const publishedMessage = await permissionPublisher.publish(message)

      const publishCommand = snsSpy.mock.calls
        .map((call) => call[0])
        .findLast((command) => command instanceof PublishCommand) as PublishCommand | undefined
      expect(publishCommand).toBeDefined()

      const publishedBody = JSON.parse(publishCommand?.input.Message as string)
      expect(publishedBody).toEqual({
        id: '1',
        messageType: 'add',
        timestamp: expect.any(String),
        _internalRetryLaterCount: 0,
      })
      expect(publishedBody).not.toHaveProperty('notInSchema')
      expect(publishedMessage).toEqual({
        id: '1',
        messageType: 'add',
        timestamp: message.timestamp,
      })
      expect(publishedMessage).not.toHaveProperty('notInSchema')
    })

    it('publishes values produced by the schema', async () => {
      const publisher = new SnsSchemaDefaultsPublisher(diContainer.cradle)
      await publisher.init()

      const snsSpy = vi.spyOn(snsClient, 'send')

      const timestamp = new Date().toISOString()
      const publishedMessage = await publisher.publish({
        id: '1',
        messageType: 'schemaDefaults',
        timestamp,
        attempt: '3',
      })

      const publishCommand = snsSpy.mock.calls
        .map((call) => call[0])
        .findLast((command) => command instanceof PublishCommand) as PublishCommand | undefined
      expect(publishCommand).toBeDefined()

      const publishedBody = JSON.parse(publishCommand?.input.Message as string)
      expect(publishedBody).toEqual({
        id: '1',
        messageType: 'schemaDefaults',
        timestamp: expect.any(String),
        source: 'default-source',
        attempt: 3,
        _internalRetryLaterCount: 0,
      })

      // The returned message carries the schema's default and coercion, and none of the
      // bookkeeping fields that only exist on the wire.
      expect(publishedMessage).toEqual({
        id: '1',
        messageType: 'schemaDefaults',
        timestamp,
        source: 'default-source',
        attempt: 3,
      })

      await publisher.close()
    })

    it('preserves message metadata in a publish error message', async () => {
      expect.assertions(1)
      const { permissionPublisher } = diContainer.cradle
      // Has to be a field declared in the publisher schema - anything else is stripped by the
      // schema parsing that precedes publishing, and the payload would no longer be oversized.
      const metadata: Record<string, string> = {}
      for (let i = 0; i < 5000; i++) {
        metadata[`key-${i}`] = 'really-long-permissions-for-testing-excessively-large-payloads'
      }

      const message = {
        id: '1',
        messageType: 'add',
        metadata,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await subscribeToTopic(
        sqsClient,
        snsClient,
        stsClient,
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
      expect(res.message).toEqual({
        ...message,
        timestamp: expect.any(String),
        _internalRetryLaterCount: 0,
      })
    })

    it('retries lazy init on the next publish after a failed one', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle)
      const initSpy = vi
        .spyOn(newPublisher, 'init')
        .mockRejectedValueOnce(new Error('Transient init failure'))

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await expect(newPublisher.publish(message)).rejects.toThrow('Transient init failure')
      await newPublisher.publish(message)

      const res = await newPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(res.message).toEqual({
        ...message,
        timestamp: expect.any(String),
        _internalRetryLaterCount: 0,
      })
      expect(initSpy).toHaveBeenCalledTimes(2)

      await newPublisher.close()
    })

    it('starts a fresh lazy init when publishing after close', async () => {
      const newPublisher = new SnsPermissionPublisher(diContainer.cradle)
      // The first attempt never settles, as if it were still in flight when close() ran
      const initSpy = vi
        .spyOn(newPublisher, 'init')
        .mockImplementationOnce(() => new Promise(() => {}))

      void newPublisher.publish({ id: '1', messageType: 'add' })
      await newPublisher.close()
      await newPublisher.publish({ id: '2', messageType: 'add' })

      const spy = await newPublisher.handlerSpy.waitForMessageWithId('2', 'published')
      expect(spy.processingResult).toEqual({ status: 'published' })
      expect(initSpy).toHaveBeenCalledTimes(2)

      await newPublisher.close()
    })
  })
})
