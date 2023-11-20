import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, expect, it, afterAll, beforeAll } from 'vitest'

import type { SQSMessage } from '../../lib/types/MessageTypes'
import { deserializeSQSMessage } from '../../lib/utils/sqsMessageDeserializer'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
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

  beforeAll(async () => {
    logger = new FakeLogger()
    diContainer = await registerDependencies({
      consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      logger: asFunction(() => logger),
    })
    publisher = diContainer.cradle.permissionPublisher
  })

  it('logs a message when logging is enabled', async () => {
    const message = {
      userIds,
      messageType: 'add',
      permissions: perms,
    } satisfies PERMISSIONS_MESSAGE_TYPE

    await publisher.publish(message)

    await waitAndRetry(() => {
      return logger.loggedMessages.length === 1
    })

    expect(logger.loggedMessages.length).toBe(1)
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let consumer: Consumer
    let publisher: SqsPermissionPublisherMonoSchema

    beforeAll(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
    })

    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      // @ts-ignore
      diContainer.cradle.permissionPublisher.deletionConfig = {
        deleteIfExists: true,
      }
      await diContainer.cradle.permissionPublisher.init()

      const command = new ReceiveMessageCommand({
        QueueUrl: publisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages!.length).toBe(0)
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes a message', async () => {
      const { permissionPublisher } = diContainer.cradle

      const message = {
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
    })
  })
})
