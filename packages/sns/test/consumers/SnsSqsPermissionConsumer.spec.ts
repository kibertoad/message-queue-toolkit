import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@lokalise/node-core'
import { assertQueue, deleteQueue, getQueueAttributes } from '@message-queue-toolkit/sqs'
import type { AwilixContainer } from 'awilix'
import { asFunction } from 'awilix'
import { describe, beforeEach, afterEach, expect, it, beforeAll } from 'vitest'

import { assertTopic, deleteTopic } from '../../lib/utils/snsUtils'
import { SnsPermissionPublisher } from '../publishers/SnsPermissionPublisher'
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
              VisibilityTimeout: '10',
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

      expect(attributes.result?.attributes).toMatchObject({
        KmsMasterKeyId: 'othervalue',
        VisibilityTimeout: '10',
      })
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

  describe('visibility timeout', () => {
    const topicName = 'myTestTopic'
    const queueName = 'myTestQueue'
    let diContainer: AwilixContainer<Dependencies>

    beforeEach(async () => {
      diContainer = await registerDependencies({
        permissionConsumer: asFunction(() => undefined),
        permissionPublisher: asFunction(() => undefined),
      })
    })

    afterEach(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it.each([false, true])('using 2 consumers with heartbeat -> %s', async (heartbeatEnabled) => {
      let consumer1IsProcessing = false
      let consumer1Counter = 0
      let consumer2Counter = 0

      const consumer1 = new SnsSqsPermissionConsumer(diContainer.cradle, {
        creationConfig: {
          topic: { Name: topicName },
          queue: { QueueName: queueName, Attributes: { VisibilityTimeout: '2' } },
        },
        consumerOverrides: { heartbeatInterval: heartbeatEnabled ? 1 : undefined },
        removeHandlerOverride: async () => {
          consumer1IsProcessing = true
          // wait for consumer2 to process message while this is still processing
          await waitAndRetry(() => consumer2Counter > 0, 100, 30)
          consumer1Counter++
          consumer1IsProcessing = false
          return { result: 'success' }
        },
      })
      await consumer1.start()

      const consumer2 = new SnsSqsPermissionConsumer(diContainer.cradle, {
        locatorConfig: {
          queueUrl: consumer1.subscriptionProps.queueUrl,
          topicArn: consumer1.subscriptionProps.topicArn,
          subscriptionArn: consumer1.subscriptionProps.subscriptionArn,
        },
        removeHandlerOverride: async () => {
          consumer2Counter++
          return { result: 'success' }
        },
      })
      const publisher = new SnsPermissionPublisher(diContainer.cradle, {
        locatorConfig: { topicArn: consumer1.subscriptionProps.topicArn },
      })

      await publisher.publish({ id: '10', messageType: 'remove' })
      // wait for consumer1 to start processing to start second consumer
      await waitAndRetry(() => consumer1IsProcessing, 5, 5)
      await consumer2.start()

      // wait for both consumers to process message
      await waitAndRetry(() => consumer1Counter > 0 && consumer2Counter > 0, 100, 40)

      expect(consumer1Counter).toBe(1)
      expect(consumer2Counter).toBe(heartbeatEnabled ? 0 : 1)
    })
  })
})
