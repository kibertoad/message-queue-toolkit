import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { assertQueue, deleteQueue, getQueueAttributes } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils'
import type { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SnsSqsPermissionConsumer } from './SnsSqsPermissionConsumer'

describe('SnsSqsPermissionConsumer', () => {
  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let snsClient: SNSClient
    beforeAll(async () => {
      diContainer = await registerDependencies({}, false)
      sqsClient = diContainer.cradle.sqsClient
      snsClient = diContainer.cradle.snsClient
    })
    beforeEach(async () => {
      await deleteQueue(sqsClient, 'existingQueue')
    })

    // FixMe https://github.com/localstack/localstack/issues/9306
    it.skip('throws an error when invalid queue locator is passed', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
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

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: {
          topicArn: arn,
          queueUrl: 'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
          subscriptionArn:
            'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://s3.localhost.localstack.cloud:4566/000000000000/existingQueue',
      )
      expect(newConsumer.subscriptionProps.topicArn).toEqual(arn)
      expect(newConsumer.subscriptionProps.subscriptionArn).toBe(
        'arn:aws:sns:eu-west-1:000000000000:user_permissions:bdf640a2-bedf-475a-98b8-758b88c87395',
      )
      await deleteTopic(snsClient, 'existingTopic')
    })

    it('updates existing queue when one with different attributes exist', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
        Attributes: {
          KmsMasterKeyId: 'somevalue',
        },
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'sometopic',
          },
          queue: {
            QueueName: 'existingQueue',
            Attributes: {
              KmsMasterKeyId: 'othervalue',
            },
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: false,
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('othervalue')
    })

    it('updates existing queue when one with different attributes exist and sets the policy', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'existingQueue',
        Attributes: {
          KmsMasterKeyId: 'somevalue',
        },
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'sometopic',
          },
          queue: {
            QueueName: 'existingQueue',
            Attributes: {
              KmsMasterKeyId: 'othervalue',
            },
          },
          updateAttributesIfExists: true,
          topicArnsWithPublishPermissionsPrefix: 'someservice-',
        },
        deletionConfig: {
          deleteIfExists: false,
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes!.Policy).toBe(
        '{"Version":"2012-10-17","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"arn:aws:sqs:eu-west-1:000000000000:existingQueue","Condition":{"ArnLike":{"aws:SourceArn":"someservice-"}}}]}',
      )
    })

    it('does not attempt to update non-existing queue when passing update param', async () => {
      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: 'sometopic',
          },
          queue: {
            QueueName: 'existingQueue',
            Attributes: {
              KmsMasterKeyId: 'othervalue',
            },
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: false,
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('othervalue')
    })

    it('creates a new dead letter queue', async () => {
      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: 'sometopic' },
          queue: { QueueName: 'existingQueue' },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          creationConfig: {
            queue: { QueueName: 'deadLetterQueue' },
            topic: { Name: 'xxxx' }, // TODO: remove topic creation
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:deadLetterQueue`,
          maxReceiveCount: 3,
        }),
      })
    })

    it('using existing dead letter queue', async () => {
      await assertQueue(sqsClient, {
        QueueName: 'deadLetterQueue',
      })

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: 'sometopic' },
          queue: { QueueName: 'existingQueue' },
          updateAttributesIfExists: true,
        },
        deadLetterQueue: {
          redrivePolicy: { maxReceiveCount: 3 },
          locatorConfig: {
            queueUrl: 'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
            topicArn: 'xxxx', // TODO: remove topic creation
          },
        },
      })

      await newConsumer.init()
      expect(newConsumer.subscriptionProps.queueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/existingQueue',
      )
      expect(newConsumer.subscriptionProps.deadLetterQueueUrl).toBe(
        'http://sqs.eu-west-1.localstack:4566/000000000000/deadLetterQueue',
      )

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newConsumer.subscriptionProps.queueUrl,
      })

      expect(attributes.result?.attributes).toMatchObject({
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: `arn:aws:sqs:eu-west-1:000000000000:deadLetterQueue`,
          maxReceiveCount: 3,
        }),
      })
    })
  })

  describe('prehandlers', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisher
    beforeEach(async () => {
      diContainer = await registerDependencies({}, false)
      publisher = diContainer.cradle.permissionPublisher
      await publisher.init()
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('processes one prehandler', async () => {
      expect.assertions(1)

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME,
          },
          queue: {
            QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME,
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.prehandlerCount).toBe(1)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 1
              : 1
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })

    it('processes two prehandlers', async () => {
      expect.assertions(1)

      const newConsumer = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME,
          },
          queue: {
            QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME,
          },
          updateAttributesIfExists: true,
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        removeHandlerOverride: async (message, _context, prehandlerOutputs) => {
          expect(prehandlerOutputs.prehandlerOutput.prehandlerCount).toBe(11)
          return {
            result: 'success',
          }
        },
        removePreHandlers: [
          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 10
              : 10
            next({
              result: 'success',
            })
          },

          (message, context, prehandlerOutput, next) => {
            prehandlerOutput.prehandlerCount = prehandlerOutput.prehandlerCount
              ? prehandlerOutput.prehandlerCount + 1
              : 1
            next({
              result: 'success',
            })
          },
        ],
      })
      await newConsumer.start()

      await publisher.publish({
        id: '2',
        messageType: 'remove',
      })

      await newConsumer.handlerSpy.waitForMessageWithId('2', 'consumed')

      await newConsumer.close()
    })
  })

  describe('consume', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisher
    let consumer: SnsSqsPermissionConsumer
    beforeEach(async () => {
      diContainer = await registerDependencies()
      publisher = diContainer.cradle.permissionPublisher
      consumer = diContainer.cradle.permissionConsumer
    })

    afterEach(async () => {
      const { awilixManager } = diContainer.cradle

      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    describe('happy path', () => {
      it('Processes messages', async () => {
        await publisher.publish({
          id: '1',
          messageType: 'add',
        })
        await publisher.publish({
          id: '2',
          messageType: 'remove',
        })
        await publisher.publish({
          id: '3',
          messageType: 'remove',
        })

        await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
        await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
        await consumer.handlerSpy.waitForMessageWithId('3', 'consumed')

        expect(consumer.addBarrierCounter).toBe(3)
        expect(consumer.addCounter).toBe(1)
        expect(consumer.removeCounter).toBe(2)
      })

      it('Handles prehandlers', async () => {
        await publisher.publish({
          id: '1',
          messageType: 'add',
        })
        await publisher.publish({
          id: '2',
          messageType: 'remove',
        })
        await publisher.publish({
          id: '3',
          messageType: 'remove',
        })

        await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
        await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
        await consumer.handlerSpy.waitForMessageWithId('3', 'consumed')

        expect(consumer.addBarrierCounter).toBe(3)
        expect(consumer.addCounter).toBe(1)
        expect(consumer.removeCounter).toBe(2)
      })
    })
  })
})
