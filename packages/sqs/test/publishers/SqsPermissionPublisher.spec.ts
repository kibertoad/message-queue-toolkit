import { ListQueueTagsCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver.ts'
import type { SQSMessage } from '../../lib/types/MessageTypes.ts'
import { deserializeSQSMessage } from '../../lib/utils/sqsMessageDeserializer.ts'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas.ts'
import { registerDependencies } from '../utils/testContext.ts'
import type { Dependencies } from '../utils/testContext.ts'

import { SqsPermissionPublisher } from './SqsPermissionPublisher.ts'

describe('SqsPermissionPublisher', () => {
  describe('init', () => {
    const queueName = 'someQueue'
    const queueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      await deleteQueue(sqsClient, queueName)
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('throws an error when invalid queue locator is passed', async () => {
      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
      })

      await expect(() => newPublisher.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueUrl,
        },
      })

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(queueUrl)
    })

    it('resolves existing queue by name', async () => {
      await assertQueue(sqsClient, {
        QueueName: queueName,
      })

      const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueName,
        },
      })

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(queueUrl)
    })

    describe('attributes update', () => {
      it('updates existing queue when one with different attributes exist', async () => {
        await assertQueue(sqsClient, {
          QueueName: queueName,
          Attributes: {
            KmsMasterKeyId: 'somevalue',
          },
        })

        const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              Attributes: {
                KmsMasterKeyId: 'othervalue',
              },
            },
            updateAttributesIfExists: true,
          },
          deletionConfig: {
            deleteIfExists: false,
          },
          logMessages: true,
        })

        const sqsSpy = vi.spyOn(sqsClient, 'send')

        await newPublisher.init()
        expect(newPublisher.queueProps.url).toBe(queueUrl)

        const updateCall = sqsSpy.mock.calls.find((entry) => {
          return entry[0].constructor.name === 'SetQueueAttributesCommand'
        })
        expect(updateCall).toBeDefined()

        const attributes = await getQueueAttributes(sqsClient, newPublisher.queueProps.url)

        expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('othervalue')
      })

      it('does not update existing queue when attributes did not change', async () => {
        await assertQueue(sqsClient, {
          QueueName: queueName,
          Attributes: {
            KmsMasterKeyId: 'somevalue',
          },
        })

        const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              Attributes: {
                KmsMasterKeyId: 'somevalue',
              },
            },
            updateAttributesIfExists: true,
          },
          deletionConfig: {
            deleteIfExists: false,
          },
          logMessages: true,
        })

        const sqsSpy = vi.spyOn(sqsClient, 'send')

        await newPublisher.init()
        expect(newPublisher.queueProps.url).toBe(queueUrl)

        const updateCall = sqsSpy.mock.calls.find((entry) => {
          return entry[0].constructor.name === 'SetQueueAttributesCommand'
        })
        expect(updateCall).toBeUndefined()

        const attributes = await getQueueAttributes(sqsClient, newPublisher.queueProps.url)

        expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('somevalue')
      })
    })

    describe('tags update', () => {
      const getTags = (queueUrl: string) =>
        sqsClient.send(new ListQueueTagsCommand({ QueueUrl: queueUrl }))

      it('updates existing queue tags when update is forced', async () => {
        const initialTags = {
          project: 'some-project',
          service: 'some-service',
          leftover: 'some-leftover',
        }
        const newTags = {
          project: 'some-project',
          service: 'changed-service',
          cc: 'some-cc',
        }

        const assertResult = await assertQueue(sqsClient, {
          QueueName: queueName,
          tags: initialTags,
        })
        const preTags = await getTags(assertResult.queueUrl)
        expect(preTags.Tags).toEqual(initialTags)

        const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              tags: newTags,
            },
            forceTagUpdate: true,
          },
          deletionConfig: {
            deleteIfExists: false,
          },
          logMessages: true,
        })

        const sqsSpy = vi.spyOn(sqsClient, 'send')

        await newPublisher.init()
        expect(newPublisher.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )

        const updateCall = sqsSpy.mock.calls.find((entry) => {
          return entry[0].constructor.name === 'TagQueueCommand'
        })
        expect(updateCall).toBeDefined()

        const postTags = await getTags(assertResult.queueUrl)
        expect(postTags.Tags).toEqual({
          ...newTags,
          leftover: 'some-leftover',
        })
      })

      it('does not update existing queue tags when update is not forced', async () => {
        const initialTags = {
          project: 'some-project',
          service: 'some-service',
          leftover: 'some-leftover',
        }

        const assertResult = await assertQueue(sqsClient, {
          QueueName: queueName,
          tags: initialTags,
        })
        const preTags = await getTags(assertResult.queueUrl)
        expect(preTags.Tags).toEqual(initialTags)

        const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
          creationConfig: {
            queue: {
              QueueName: queueName,
              tags: { service: 'changed-service' },
            },
          },
          deletionConfig: {
            deleteIfExists: false,
          },
          logMessages: true,
        })

        const sqsSpy = vi.spyOn(sqsClient, 'send')

        await newPublisher.init()
        expect(newPublisher.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )

        const updateCall = sqsSpy.mock.calls.find((entry) => {
          return entry[0].constructor.name === 'TagQueueCommand'
        })
        expect(updateCall).toBeUndefined()

        const postTags = await getTags(assertResult.queueUrl)
        expect(postTags.Tags).toEqual(initialTags)
      })
    })
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let permissionPublisher: SqsPermissionPublisher

    beforeEach(async () => {
      diContainer = await registerDependencies()
      await diContainer.cradle.permissionConsumer.close()
      permissionPublisher = diContainer.cradle.permissionPublisher
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publish invalid message', async () => {
      await expect(
        permissionPublisher.publish({
          id: '10',
          messageType: 'bad' as any,
        }),
      ).rejects.toThrow(/Unsupported message type: bad/)
    })

    // See https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
    it('publish message with payload containing forbidden unicode character', async () => {
      await expect(
        permissionPublisher.publish({
          id: '\uFFFF',
          messageType: 'add',
          timestamp: new Date().toISOString(),
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE),
      ).rejects.toThrow(/Error while publishing to SQS: Invalid characters found/)
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      const spy = await permissionPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toEqual({ status: 'published' })
    })

    it('publish a message auto-filling internal properties', async () => {
      const QueueName = 'auto-filling_test_queue'
      const { queueUrl } = await assertQueue(diContainer.cradle.sqsClient, {
        QueueName,
      })

      const permissionPublisher = new SqsPermissionPublisher(diContainer.cradle, {
        creationConfig: {
          queue: { QueueName },
        },
      })

      let receivedMessage: unknown
      const consumer = Consumer.create({
        queueUrl: queueUrl,
        handleMessage: (message: SQSMessage) => {
          if (message === null) {
            return Promise.resolve()
          }
          const decodedMessage = deserializeSQSMessage(
            message as any,
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )
          receivedMessage = decodedMessage.result!
          return Promise.resolve()
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)
      expect(receivedMessage).toEqual({
        originalMessage: {
          id: '1',
          messageType: 'add',
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
      await permissionPublisher.close()
    })

    it('publish message with lazy loading', async () => {
      const newPublisher = new SqsPermissionPublisher(diContainer.cradle)

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await newPublisher.publish(message)

      const spy = await newPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toEqual({ status: 'published' })
    })
  })
})
