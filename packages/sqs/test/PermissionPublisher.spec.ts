import type { SQSClient } from '@aws-sdk/client-sqs'
import { ReceiveMessageCommand } from '@aws-sdk/client-sqs'
import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it, afterAll, beforeAll } from 'vitest'

import type { SQSMessage } from '../lib/sqs/AbstractSqsConsumer'
import { deserializeMessage } from '../lib/sqs/messageDeserializer'

import { PermissionConsumer } from './PermissionConsumer'
import { PermissionPublisher } from './PermissionPublisher'
import { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver'
import { userPermissionMap } from './repositories/PermissionRepository'
import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'
import { deleteQueue, purgeQueue } from './utils/sqsUtils'
import { registerDependencies, SINGLETON_CONFIG } from './utils/testContext'
import type { Dependencies } from './utils/testContext'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

describe('PermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let sqsClient: SQSClient
    let consumer: Consumer
    let publisher: PermissionPublisher

    beforeAll(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      sqsClient = diContainer.cradle.sqsClient
      publisher = diContainer.cradle.permissionPublisher
      await purgeQueue(sqsClient, PermissionConsumer.QUEUE_NAME)
    })

    beforeEach(async () => {
      delete userPermissionMap[100]
      delete userPermissionMap[200]
      delete userPermissionMap[300]

      await deleteQueue(sqsClient, PermissionPublisher.QUEUE_NAME)
      await diContainer.cradle.permissionPublisher.init()

      const command = new ReceiveMessageCommand({
        QueueUrl: publisher.queueUrl,
      })
      const reply = await sqsClient.send(command)
      expect(reply.Messages).toBeUndefined()
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    afterEach(async () => {
      consumer?.stop()
      consumer?.stop({ abort: true })
      await purgeQueue(sqsClient, PermissionPublisher.QUEUE_NAME)
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
          const decodedMessage = deserializeMessage(
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
