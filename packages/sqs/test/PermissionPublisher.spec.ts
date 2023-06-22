import { waitAndRetry } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asClass, Lifetime } from 'awilix'
import { Consumer } from 'sqs-consumer'
import { describe, beforeEach, afterEach, expect, it } from 'vitest'

import type { SQSMessage } from '../lib/sqs/AbstractSqsConsumer'
import { deserializeMessage } from '../lib/sqs/messageDeserializer'

import { PermissionConsumer } from './PermissionConsumer'
import { FakeConsumer } from './fakes/FakeConsumer'
import { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver'
import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'
import { deleteQueue } from './utils/sqsUtils'
import { registerDependencies, SINGLETON_CONFIG } from './utils/testContext'
import type { Dependencies } from './utils/testContext'

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

describe('PermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    beforeEach(async () => {
      diContainer = await registerDependencies({
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
        permissionConsumer: asClass(FakeConsumer, {
          lifetime: Lifetime.SINGLETON,
          asyncInit: 'consume',
          asyncDispose: 'close',
          asyncDisposePriority: 10,
        }),
      })
      await deleteQueue(diContainer.cradle.sqsClient, PermissionConsumer.QUEUE_NAME)
      await diContainer.cradle.permissionPublisher.init()
    })

    afterEach(async () => {
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
      const app = Consumer.create({
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
      app.start()

      await permissionPublisher.publish(message)

      await waitAndRetry(() => {
        return receivedMessage !== null
      })

      expect(receivedMessage).toEqual({
        messageType: 'add',
        permissions: ['perm1', 'perm2'],
        userIds: [100, 200, 300],
      })

      app.stop()
    })
  })
})
