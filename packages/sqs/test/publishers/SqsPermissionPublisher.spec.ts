import { ListQueueTagsCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver.ts'
import {
  SQS_RESOURCE_CURRENT_QUEUE,
  type SQSPolicyConfig,
} from '../../lib/sqs/AbstractSqsService.ts'
import type { SQSMessage } from '../../lib/types/MessageTypes.ts'
import { deserializeSQSMessage } from '../../lib/utils/sqsMessageDeserializer.ts'
import { assertQueue, getQueueAttributes } from '../../lib/utils/sqsUtils.ts'
import type { PERMISSIONS_ADD_MESSAGE_TYPE } from '../consumers/userConsumerSchemas.ts'
import { PERMISSIONS_ADD_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas.ts'
import type { TestAwsResourceAdmin } from '../utils/testAdmin.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { SqsPermissionPublisher } from './SqsPermissionPublisher.ts'

describe('SqsPermissionPublisher', () => {
  describe('init', () => {
    const queueName = 'someQueue'
    const queueUrl = `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let testAdmin: TestAwsResourceAdmin
    beforeEach(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
      testAdmin = diContainer.cradle.testAdmin
      await testAdmin.purge(queueName)
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

        expect(attributes.result?.attributes?.KmsMasterKeyId).toBe('othervalue')
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

        expect(attributes.result?.attributes?.KmsMasterKeyId).toBe('somevalue')
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

      describe('policy config', () => {
        it('creates queue without policy when policyConfig is undefined', async () => {
          const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
            creationConfig: {
              queue: {
                QueueName: queueName,
                Attributes: {
                  VisibilityTimeout: '30',
                },
              },
              policyConfig: undefined,
            },
          })

          await newPublisher.init()

          // Verify queue was created
          expect(newPublisher.queueProps.url).toBe(queueUrl)
          expect(newPublisher.queueProps.name).toBe(queueName)

          // Verify no policy was applied
          const attributes = await getQueueAttributes(sqsClient, newPublisher.queueProps.url)
          const policy = attributes.result?.attributes?.Policy

          expect(policy).toBeUndefined()

          await newPublisher.close()
        })

        it('creates queue with policy config', async () => {
          const policyConfig: SQSPolicyConfig = {
            resource: SQS_RESOURCE_CURRENT_QUEUE,
            statements: {
              Effect: 'Allow',
              Principal: 'arn:aws:iam::123456789012:user/test-user',
              Action: ['sqs:SendMessage', 'sqs:ReceiveMessage'],
            },
          }

          const newPublisher = new SqsPermissionPublisher(diContainer.cradle, {
            creationConfig: {
              queue: {
                QueueName: queueName,
                Attributes: {
                  VisibilityTimeout: '30',
                },
              },
              policyConfig,
            },
          })

          await newPublisher.init()

          // Verify queue was created
          expect(newPublisher.queueProps.url).toBe(queueUrl)

          // Verify policy was applied with current queue ARN as resource
          const attributes = await getQueueAttributes(sqsClient, newPublisher.queueProps.url)
          const policy = JSON.parse(attributes.result?.attributes?.Policy || '{}')

          expect(policy.Version).toBe('2012-10-17')
          expect(policy.Statement[0].Resource).toBe(newPublisher.queueProps.arn)
          expect(policy).toMatchInlineSnapshot(`
            {
              "Statement": [
                {
                  "Action": [
                    "sqs:SendMessage",
                    "sqs:ReceiveMessage",
                  ],
                  "Effect": "Allow",
                  "Principal": {
                    "AWS": "arn:aws:iam::123456789012:user/test-user",
                  },
                  "Resource": "arn:aws:sqs:eu-west-1:000000000000:someQueue",
                },
              ],
              "Version": "2012-10-17",
            }
          `)

          await newPublisher.close()
        })

        it('updates existing queue policy when publisher is reinitialized with updateAttributesIfExists', async () => {
          const initialPublisher = new SqsPermissionPublisher(diContainer.cradle, {
            creationConfig: {
              queue: {
                QueueName: queueName,
                Attributes: {
                  VisibilityTimeout: '30',
                },
              },
              policyConfig: {
                resource: 'arn:aws:sqs:*:*:*',
                statements: {
                  Effect: 'Allow',
                  Principal: 'arn:aws:iam::123456789012:user/initial-user',
                  Action: ['sqs:SendMessage'],
                },
              },
            },
          })

          await initialPublisher.init()
          await initialPublisher.close()

          const updatedPublisher = new SqsPermissionPublisher(diContainer.cradle, {
            creationConfig: {
              queue: {
                QueueName: queueName,
                Attributes: {
                  VisibilityTimeout: '30',
                },
              },
              policyConfig: { resource: '*' },
              updateAttributesIfExists: true,
            },
          })

          await updatedPublisher.init()

          // Verify updated policy was applied
          const attributes = await getQueueAttributes(sqsClient, updatedPublisher.queueProps.url)
          const policy = JSON.parse(attributes.result?.attributes?.Policy || '{}')
          expect(policy.Statement[0].Resource).toBe('*')
        })
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
            return Promise.resolve(message)
          }
          const decodedMessage = deserializeSQSMessage(
            message as any,
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )
          receivedMessage = decodedMessage.result!
          return Promise.resolve(message)
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
