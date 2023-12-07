import type { SQSClient } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, expect, it, afterEach } from 'vitest'

import { FakeConsumerErrorResolver } from '../../lib/fakes/FakeConsumerErrorResolver'
import type { SQSMessage } from '../../lib/types/MessageTypes'
import { deserializeSQSMessage } from '../../lib/utils/sqsMessageDeserializer'
import { assertQueue, deleteQueue } from '../../lib/utils/sqsUtils'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakeLogger } from '../fakes/FakeLogger'
import { userPermissionMap } from '../repositories/PermissionRepository'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'
import type { Dependencies } from '../utils/testContext'

import type { SqsPermissionPublisherMonoSchema } from './SqsPermissionPublisherMonoSchema'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

describe('SqsPermissionPublisher', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisher: SqsPermissionPublisherMonoSchema
  let logger: FakeLogger
  let sqsClient: SQSClient
  let consumer: Consumer

  beforeEach(async () => {
    logger = new FakeLogger()
    diContainer = await registerDependencies({
      consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      logger: asFunction(() => logger),
    })
    publisher = diContainer.cradle.permissionPublisher
    sqsClient = diContainer.cradle.sqsClient
  })

  afterEach(async () => {
    await diContainer.cradle.awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('logging', () => {
    it('logs a message when logging is enabled', async () => {
      const message = {
        id: '1',
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      await publisher.publish(message)

      await publisher.handlerSpy.waitForMessageWithId('1')

      expect(logger.loggedMessages.length).toBe(1)
    })
  })

  describe('publish', () => {
    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      await deleteQueue(sqsClient, diContainer.cradle.permissionPublisher.queueName)
      // @ts-ignore
      await assertQueue(sqsClient, diContainer.cradle.permissionPublisher.creationConfig!.queue)
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
        id: '2',
        userIds,
        messageType: 'add',
        permissions: perms,
      } satisfies PERMISSIONS_MESSAGE_TYPE

      let receivedMessage: PERMISSIONS_MESSAGE_TYPE | null = null
      consumer = Consumer.create({
        queueUrl: diContainer.cradle.permissionPublisher.queueUrl,
        handleMessage: async (message: SQSMessage) => {
          if (message === null) {
            return
          }
          const decodedMessage = deserializeSQSMessage(
            message as any,
            PERMISSIONS_MESSAGE_SCHEMA,
            new FakeConsumerErrorResolver(),
          )
          receivedMessage = decodedMessage.result!
        },
        sqs: diContainer.cradle.sqsClient,
      })
      consumer.start()

      consumer.on('error', () => {})

      await permissionPublisher.publish(message)

      await waitAndRetry(() => {
        return receivedMessage !== null
      })

      expect(receivedMessage).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })

      consumer.stop()
    })
  })
})
