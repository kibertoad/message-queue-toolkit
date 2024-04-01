import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import type { AwilixContainer } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { beforeEach, describe, expect, it } from 'vitest'

import type { SQSMessage } from '../../lib/types/MessageTypes'
import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import { SqsPermissionConsumer } from './SqsPermissionConsumer'

describe('SqsPermissionConsumer - deadLetterQueue', () => {
  describe('init', () => {
    const queueName = 'sqsTestQueue'
    const deadLetterQueueName = 'customDlq'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient

    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
    })

    beforeEach(async () => {
      await deleteQueue(sqsClient, queueName)
      await deleteQueue(sqsClient, deadLetterQueueName)
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    describe('creating new dead letter queue', () => {
      it('creates dead letter', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { QueueName: deadLetterQueueName } },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue for an existing queue', async () => {
        const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })

        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl },
          deadLetterQueue: {
            creationConfig: { queue: { QueueName: deadLetterQueueName } },
            redrivePolicy: { maxReceiveCount: 5 },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(queueUrl)
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${deadLetterQueueName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })
    })

    describe('existing dead letter queue', () => {
      let dlqUrl: string
      beforeEach(async () => {
        const result = await assertQueue(sqsClient, {
          QueueName: deadLetterQueueName,
          Attributes: { KmsMasterKeyId: 'my first value' },
        })
        dlqUrl = result.queueUrl
      })

      it('throws an error when invalid dlq locator is passed', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: {
              queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/badQueue`,
            },
          },
        })

        await expect(() => newConsumer.init()).rejects.toThrow(/does not exist/)
      })

      it('does not create a new queue when dlq locator is passed', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: {
              queueUrl: dlqUrl,
            },
          },
        })

        await newConsumer.init()
        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(dlqUrl)
      })

      it('updates existing dlq when one with different attributes exist', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName }, updateAttributesIfExists: true },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: {
              updateAttributesIfExists: true,
              queue: {
                QueueName: deadLetterQueueName,
                Attributes: { KmsMasterKeyId: 'new value' },
              },
            },
          },
        })

        await newConsumer.init()
        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(dlqUrl)

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.dlqUrl,
        })

        expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('new value')
      })

      it('connect existing dlq to existing queue', async () => {
        const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })

        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            locatorConfig: { queueUrl: dlqUrl },
          },
        })

        await newConsumer.init()
        expect(newConsumer.queueProps.url).toBe(queueUrl)
        expect(newConsumer.dlqUrl).toBe(dlqUrl)

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl,
        })

        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${deadLetterQueueName}`,
            maxReceiveCount: 5,
          }),
        })
      })
    })
  })

  describe('messages with errors on process should go to DLQ', () => {
    const queueName = SqsPermissionConsumer.QUEUE_NAME
    const deadLetterQueueName = `${queueName}-dlq`

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient

    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
    })

    beforeEach(async () => {
      await deleteQueue(sqsClient, queueName)
      await deleteQueue(sqsClient, deadLetterQueueName)
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('after errors, messages should go to DLQ', async () => {
      const { permissionPublisher } = diContainer.cradle
      let counter = 0
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 2 },
        },
        removeHandlerOverride: async () => {
          counter++
          throw new Error('Error')
        },
      })
      await consumer.start()

      let dlqMessage: any
      const dlqConsumer = Consumer.create({
        sqs: diContainer.cradle.sqsClient,
        queueUrl: consumer.dlqUrl,
        handleMessage: async (message: SQSMessage) => {
          dlqMessage = message
        },
      })
      dlqConsumer.start()

      await permissionPublisher.publish({ id: '1', messageType: 'remove' })

      await waitAndRetry(async () => dlqMessage, 50, 20)

      expect(counter).toBe(2)
      expect(dlqMessage.Body).toBe(JSON.stringify({ id: '1', messageType: 'remove' }))
    })

    // TODO: TDD -> in case of retryLater, DLQ shouldn't be used and message should be finally processed
    it.skip('messages with retryLater should always be retried and not go to DLQ', async () => {
      const { permissionPublisher } = diContainer.cradle
      let counter = 0
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        creationConfig: { queue: { QueueName: queueName } },
        deadLetterQueue: {
          creationConfig: { queue: { QueueName: deadLetterQueueName } },
          redrivePolicy: { maxReceiveCount: 3 },
        },
        removeHandlerOverride: async () => {
          counter++
          if (counter < 10) {
            return { error: 'retryLater' }
          }
          return { result: 'success' }
        },
      })
      await consumer.start()

      await permissionPublisher.publish({ id: '1', messageType: 'remove' })

      const handlerSpyResult = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(handlerSpyResult.processingResult).toBe('consumed')
      expect(handlerSpyResult.message).toMatchObject({ id: '1', messageType: 'remove' })
    })

    it('messages with deserialization errors should go to DLQ', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
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
        handleMessage: async (message: SQSMessage) => {
          dlqMessage = message
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
})
