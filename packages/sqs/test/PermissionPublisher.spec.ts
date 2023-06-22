import type { AwilixContainer } from 'awilix'
import { asClass, Lifetime } from 'awilix'
import { describe, beforeAll, beforeEach, afterAll, afterEach, expect, it } from 'vitest'

import { PermissionPublisher } from './PermissionPublisher'
import {Dependencies, registerDependencies, SINGLETON_CONFIG} from "./utils/testContext";
import {FakeConsumerErrorResolver} from "./fakes/FakeConsumerErrorResolver";
import {FakeConsumer} from "./fakes/FakeConsumer";
import {PermissionConsumer} from "./PermissionConsumer";
import {PERMISSIONS_MESSAGE_SCHEMA, PERMISSIONS_MESSAGE_TYPE} from "./userConsumerSchemas";
import {deserializeMessage} from "../lib/sqs/messageDeserializer";
import { waitAndRetry} from '@message-queue-toolkit/core'
import { Consumer} from 'sqs-consumer'
import {SQSMessage} from "../lib/sqs/AbstractSqsConsumer";
import {deleteQueue} from "./utils/sqsUtils";

const perms: [string, ...string[]] = ['perm1', 'perm2']
const userIds = [100, 200, 300]

describe('PermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    beforeAll(async () => {
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
      const app = Consumer.create({
        queueUrl: PermissionConsumer.QUEUE_NAME,
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
