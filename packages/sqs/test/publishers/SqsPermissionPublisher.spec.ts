import type { SQSClient } from '@aws-sdk/client-sqs'
import type { AwilixContainer } from 'awilix'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { assertQueue, deleteQueue, getQueueAttributes } from '../../lib/utils/sqsUtils'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import { registerDependencies } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import { SqsPermissionPublisher } from './SqsPermissionPublisher'

describe('SqsPermissionPublisher', () => {
  describe('init', () => {
    const queueName = 'someQueue'

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
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
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
          queueUrl: `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
        },
      })

      await newPublisher.init()
      expect(newPublisher.queueProps.url).toBe(
        `http://s3.localhost.localstack.cloud:4566/000000000000/${queueName}`,
      )
    })

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
      expect(newPublisher.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeDefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newPublisher.queueProps.url,
      })

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
      expect(newPublisher.queueProps.url).toBe(
        `http://sqs.eu-west-1.localstack:4566/000000000000/${queueName}`,
      )

      const updateCall = sqsSpy.mock.calls.find((entry) => {
        return entry[0].constructor.name === 'SetQueueAttributesCommand'
      })
      expect(updateCall).toBeUndefined()

      const attributes = await getQueueAttributes(sqsClient, {
        queueUrl: newPublisher.queueProps.url,
      })

      expect(attributes.result?.attributes!.KmsMasterKeyId).toBe('somevalue')
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

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        userIds: [100, 200, 300],
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      const spy = await permissionPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toBe('published')
    }, 99999999)

    it('publish a message auto-filling timestamp', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await permissionPublisher.publish(message)

      const spy = await permissionPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual({ ...message, timestamp: expect.any(String) })
      expect(spy.processingResult).toBe('published')
    })

    it('publish message with lazy loading', async () => {
      const newPublisher = new SqsPermissionPublisher(diContainer.cradle)

      const message = {
        id: '1',
        userIds: [100, 200, 300],
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
      } satisfies PERMISSIONS_MESSAGE_TYPE

      await newPublisher.publish(message)

      const spy = await newPublisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toBe('published')
    })
  })
})
