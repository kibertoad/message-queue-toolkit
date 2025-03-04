import { ListQueueTagsCommand, type SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import type { SQSMessage } from '../../lib/types/MessageTypes'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import type { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import { SqsPermissionConsumer } from './SqsPermissionConsumer'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'

describe('SqsPermissionConsumer - deadLetterQueue', () => {
  const queueName = SqsPermissionConsumer.QUEUE_NAME
  const deadLetterQueueName = `${queueName}-dlq`

  let diContainer: AwilixContainer<Dependencies>
  let sqsClient: SQSClient
  let permissionPublisher: SqsPermissionPublisher
  let consumer: SqsPermissionConsumer | undefined

  beforeAll(async () => {
    diContainer = await registerDependencies()
    sqsClient = diContainer.cradle.sqsClient
    permissionPublisher = diContainer.cradle.permissionPublisher
  })

  beforeEach(async () => {
    await deleteQueue(sqsClient, queueName)
    await deleteQueue(sqsClient, deadLetterQueueName)
  })

  afterEach(async () => {
    await consumer?.close()
  })

  afterAll(async () => {
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('init', () => {
    const customQueueName = 'sqsTestQueue'
    const customDeadLetterQueueName = 'customDlq'

    beforeEach(async () => {
      await deleteQueue(sqsClient, customQueueName)
      await deleteQueue(sqsClient, customDeadLetterQueueName)
    })

    describe('creating new dead letter queue', () => {
      it('creates dead letter', async () => {
        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: customQueueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { QueueName: customDeadLetterQueueName } },
          },
        })

        await consumer.init()

        expect(consumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customQueueName}`,
        )
        expect(consumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customDeadLetterQueueName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, consumer.queueProps.url)
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDeadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue for an existing queue', async () => {
        const { queueUrl } = await assertQueue(sqsClient, { QueueName: customQueueName })

        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl },
          deadLetterQueue: {
            creationConfig: { queue: { QueueName: customDeadLetterQueueName } },
            redrivePolicy: { maxReceiveCount: 5 },
          },
        })

        await consumer.init()

        expect(consumer.queueProps.url).toBe(queueUrl)
        expect(consumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customDeadLetterQueueName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, consumer.queueProps.url)
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDeadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })
    })

    describe('existing dead letter queue', () => {
      let dlqUrl: string

      beforeEach(async () => {
        const result = await assertQueue(sqsClient, {
          QueueName: customDeadLetterQueueName,
          Attributes: { KmsMasterKeyId: 'my first value' },
          tags: { tag: 'old', hello: 'world' },
        })
        dlqUrl = result.queueUrl
      })

      it('throws an error when invalid dlq locator is passed', async () => {
        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: customQueueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: {
              queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/badQueue`,
            },
          },
        })

        await expect(() => consumer?.init()).rejects.toThrow(/does not exist/)
      })

      it('does not create a new queue when dlq locator is passed', async () => {
        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: customQueueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: {
              queueUrl: dlqUrl,
            },
          },
        })

        await consumer.init()
        expect(consumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customQueueName}`,
        )
        expect(consumer.dlqUrl).toBe(dlqUrl)
      })

      it('updates existing dlq attributes', async () => {
        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: customQueueName }, updateAttributesIfExists: true },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: {
              updateAttributesIfExists: true,
              queue: {
                QueueName: customDeadLetterQueueName,
                Attributes: { KmsMasterKeyId: 'new value' },
              },
            },
          },
        })

        await consumer.init()
        expect(consumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customQueueName}`,
        )
        expect(consumer.dlqUrl).toBe(dlqUrl)

        const attributes = await getQueueAttributes(sqsClient, consumer.dlqUrl)

        expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('new value')
      })

      it('updates existing dlq tags', async () => {
        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: customQueueName }, updateAttributesIfExists: true },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: {
              forceTagUpdate: true,
              queue: {
                QueueName: customDeadLetterQueueName,
                tags: { tag: 'new', good: 'bye' },
              },
            },
          },
        })

        await consumer.init()
        expect(consumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customQueueName}`,
        )
        expect(consumer.dlqUrl).toBe(dlqUrl)

        const tags = await sqsClient.send(new ListQueueTagsCommand({ QueueUrl: consumer.dlqUrl }))
        expect(tags.Tags).toMatchInlineSnapshot(`
          {
            "good": "bye",
            "hello": "world",
            "tag": "new",
          }
        `)
      })

      it('connect existing dlq to existing queue', async () => {
        const { queueUrl } = await assertQueue(sqsClient, { QueueName: customQueueName })

        consumer = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: { queueUrl: dlqUrl },
          },
        })

        await consumer.init()
        expect(consumer.queueProps.url).toBe(queueUrl)
        expect(consumer.dlqUrl).toBe(dlqUrl)

        const attributes = await getQueueAttributes(sqsClient, queueUrl)

        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDeadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })
    })
  })

  describe('messages with errors on process should go to DLQ', () => {
    it('after errors, messages should go to DLQ', async () => {
      let counter = 0
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 2 },
        },
        removeHandlerOverride: () => {
          counter++
          throw new Error('Error')
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.dlqUrl,
        handleMessage: (message: SQSMessage) => {
          dlqMessage = message
          return Promise.resolve()
        },
      })
      dlqConsumer.start()

      await permissionPublisher.publish({ id: '1', messageType: 'remove' })

      await waitAndRetry(() => dlqMessage, 50, 20)

      expect(counter).toBe(2)
      expect(JSON.parse(dlqMessage.Body)).toEqual({
        id: '1',
        messageType: 'remove',
        timestamp: expect.any(String),
        _internalRetryLaterCount: 0,
      })
    })

    it('messages with retryLater should be retried with exponential delay and not go to DLQ', async () => {
      const sqsMessage: PERMISSIONS_REMOVE_MESSAGE_TYPE = { id: '1', messageType: 'remove' }

      let counter = 0
      const messageArrivalTime: number[] = []
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 1 },
        },
        removeHandlerOverride: (message) => {
          if (message.id !== sqsMessage.id) {
            throw new Error('not expected message')
          }
          counter++
          messageArrivalTime.push(new Date().getTime())
          return counter < 2
            ? Promise.resolve({ error: 'retryLater' })
            : Promise.resolve({ result: 'success' })
        },
      })
      await consumer.start()

      await permissionPublisher.publish(sqsMessage)

      const handlerSpyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(handlerSpyResult.processingResult).toBe('consumed')
      expect(handlerSpyResult.message).toMatchObject({ id: '1', messageType: 'remove' })

      expect(counter).toBe(2)

      // delay is 1s, but consumer can take the message
      const secondsRetry = (messageArrivalTime[1] - messageArrivalTime[0]) / 1000
      expect(secondsRetry).toBeGreaterThan(1)
    })

    it('messages with deserialization errors should go to DLQ', async () => {
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 1 },
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.dlqUrl,
        handleMessage: (message: SQSMessage) => {
          dlqMessage = message
          return Promise.resolve()
        },
      })
      dlqConsumer.start()

      // not using publisher to avoid publisher validation
      await sqsClient.send(
        new SendMessageCommand({
          QueueUrl: consumer.queueProps.url,
          MessageBody: JSON.stringify({ id: '1', messageType: 'bad' }),
        }),
      )

      await waitAndRetry(async () => dlqMessage, 20, 5)

      expect(dlqMessage.Body).toBe(JSON.stringify({ id: '1', messageType: 'bad' }))
    })
  })

  describe('messages stuck should be marked as consumed and go to DLQ', () => {
    it('messages stuck on barrier', async () => {
      let counter = 0
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 200 },
        },
        maxRetryDuration: 2,
        addPreHandlerBarrier: (_msg) => {
          counter++
          return Promise.resolve({ isPassing: false })
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.dlqUrl,
        handleMessage: (message: SQSMessage) => {
          dlqMessage = message
          return Promise.resolve()
        },
      })
      dlqConsumer.start()

      const message: PERMISSIONS_ADD_MESSAGE_TYPE = {
        id: '1',
        messageType: 'add',
        timestamp: new Date(new Date().getTime() - 1000).toISOString(),
      }
      await permissionPublisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spyResult.message).toEqual(message)
      // due to exponential backoff and timestamp, message is only retried once before being moved to DLQ
      expect(counter).toBe(2)

      await waitAndRetry(() => dlqMessage)
      const messageBody = JSON.parse(dlqMessage.Body)
      expect(messageBody).toEqual({
        id: '1',
        messageType: 'add',
        timestamp: message.timestamp,
        _internalRetryLaterCount: 1,
      })

      dlqConsumer.stop()
    })

    it('messages stuck on handler', async () => {
      let counter = 0
      consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 200 },
        },
        maxRetryDuration: 2,
        removeHandlerOverride: () => {
          counter++
          return Promise.resolve({ error: 'retryLater' })
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.dlqUrl,
        handleMessage: (message: SQSMessage) => {
          dlqMessage = message
          return Promise.resolve()
        },
      })
      dlqConsumer.start()

      const message: PERMISSIONS_REMOVE_MESSAGE_TYPE = {
        id: '2',
        messageType: 'remove',
        timestamp: new Date(new Date().getTime() - 1000).toISOString(),
      }
      await permissionPublisher.publish(message)

      const spyResult = await consumer.handlerSpy.waitForMessageWithId('2', 'error')
      expect(spyResult.message).toEqual(message)
      // due to exponential backoff and timestamp, message is only retried once before being moved to DLQ
      expect(counter).toBe(2)

      await waitAndRetry(() => dlqMessage)
      const messageBody = JSON.parse(dlqMessage.Body)
      expect(messageBody).toEqual({
        id: '2',
        messageType: 'remove',
        timestamp: message.timestamp,
        _internalRetryLaterCount: 1,
      })

      dlqConsumer.stop()
    }, 7000)
  })
})
