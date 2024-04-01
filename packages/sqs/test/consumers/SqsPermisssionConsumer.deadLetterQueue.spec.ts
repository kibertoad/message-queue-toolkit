import type { SQSClient } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { beforeEach, describe, expect, it } from 'vitest'

import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import { SqsPermissionConsumer } from './SqsPermissionConsumer'

describe('dead letter queue', () => {
  describe('init', () => {
    const customDlqName = 'customDlq'
    const customSuffix = '-customSuffix'
    const queueName = 'sqsTestQueue'

    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient

    beforeAll(async () => {
      diContainer = await registerDependencies()
      sqsClient = diContainer.cradle.sqsClient
    })

    beforeEach(async () => {
      await deleteQueue(sqsClient, queueName)
      await deleteQueue(sqsClient, customDlqName)
      await deleteQueue(sqsClient, `${queueName}-dlq`)
      await deleteQueue(sqsClient, `${queueName}${customSuffix}`)
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    describe('creating new dead letter queue', () => {
      it('creates dead letter queue using default name', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: { redrivePolicy: { maxReceiveCount: 5 } },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}-dlq`,
        )
        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${queueName}-dlq`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue using custom suffix', async () => {
        const suffix = '-test'
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { queueNameSuffix: suffix } },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}${suffix}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${queueName}${suffix}`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue using custom name', async () => {
        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          creationConfig: { queue: { QueueName: queueName } },
          deadLetterQueue: {
            redrivePolicy: { maxReceiveCount: 5 },
            creationConfig: { queue: { QueueName: customDlqName } },
          },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
        )
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${customDlqName}`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDlqName}`,
            maxReceiveCount: 5,
          }),
        })
      })

      it('creates dead letter queue for an existing queue', async () => {
        const { queueUrl } = await assertQueue(sqsClient, { QueueName: queueName })

        const newConsumer = new SqsPermissionConsumer(diContainer.cradle, {
          locatorConfig: { queueUrl },
          deadLetterQueue: { redrivePolicy: { maxReceiveCount: 5 } },
        })

        await newConsumer.init()

        expect(newConsumer.queueProps.url).toBe(queueUrl)
        expect(newConsumer.dlqUrl).toBe(
          `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}-dlq`,
        )

        const attributes = await getQueueAttributes(sqsClient, {
          queueUrl: newConsumer.queueProps.url,
        })
        expect(attributes.result?.attributes).toMatchObject({
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${queueName}-dlq`,
            maxReceiveCount: 5,
          }),
        })
      })
    })

    describe('existing dead letter queue', () => {
      let dlqUrl: string
      beforeEach(async () => {
        const result = await assertQueue(sqsClient, {
          QueueName: customDlqName,
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
                QueueName: customDlqName,
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

      it('connection existing dlq to existing queue', async () => {
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
            deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:${customDlqName}`,
            maxReceiveCount: 5,
          }),
        })
      })
    })
  })

  // eslint-disable-next-line vitest/no-commented-out-tests
  /*
    describe('messages with errors on process should go to DLQ', () => {
      let diContainer: AwilixContainer<Dependencies>
      let publisher: SqsPermissionPublisherMultiSchema
      let sqsClient: SQSClient

      beforeAll(async () => {
        diContainer = await registerDependencies()
        // eslint-disable-next-line max-lines
        sqsClient = diContainer.cradle.sqsClient
        publisher = diContainer.cradle.permissionPublisherMultiSchema
      })

      afterEach(async () => {
        await diContainer.cradle.awilixManager.executeDispose()
        await diContainer.dispose()
      })

      beforeEach(async () => {
        await deleteQueue(sqsClient, publisher.queueName)
        await deleteQueue(sqsClient, `${publisher.queueName}-dlq`)
      })

      it('after errors, messages should go to DLQ', async () => {
        let counter = 0
        const consumer = new SqsPermissionConsumerMultiSchema(diContainer.cradle, {
          creationConfig: { queue: { QueueName: publisher.queueName } },
          deadLetterQueue: { redrivePolicy: { maxReceiveCount: 2 } },

          removeHandlerOverride: async () => {
            counter++
            throw new Error('Error')
          },
        })
        await consumer.start()

        let dlqMessage: any
        const dlqConsumer = Consumer.create({
          sqs: diContainer.cradle.sqsClient,
          queueUrl: consumer.deadLetterQueueUrl!,
          handleMessage: async (message: SQSMessage) => {
            dlqMessage = message
          },
        })
        dlqConsumer.start()

        await publisher.publish({ id: '1', messageType: 'remove' })

        await waitAndRetry(async () => dlqMessage, 20, 5)

        expect(counter).toBe(2)
        expect(dlqMessage.Body).toBe(JSON.stringify({ id: '1', messageType: 'remove' }))
      })
    })
     */
})
