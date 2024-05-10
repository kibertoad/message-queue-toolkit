import type { InternalError } from '@lokalise/node-core'
import { waitAndRetry } from '@lokalise/node-core'
import type { Channel } from 'amqplib'
import type { AwilixContainer } from 'awilix'
import { asClass, asFunction, Lifetime } from 'awilix'
import { describe, beforeAll, beforeEach, afterAll, afterEach, expect, it } from 'vitest'
import { ZodError } from 'zod'

import { deserializeAmqpMessage } from '../../lib/amqpMessageDeserializer'
import { AmqpPermissionConsumer } from '../consumers/AmqpPermissionConsumer'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_MESSAGE_SCHEMA,
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'
import { FakeConsumer } from '../fakes/FakeConsumer'
import { FakeConsumerErrorResolver } from '../fakes/FakeConsumerErrorResolver'
import { FakeLogger } from '../fakes/FakeLogger'
import { TEST_AMQP_CONFIG } from '../utils/testAmqpConfig'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies, SINGLETON_CONFIG } from '../utils/testContext'

import { AmqpPermissionPublisher } from './AmqpPermissionPublisher'

describe('PermissionPublisher', () => {
  describe('logging', () => {
    let logger: FakeLogger
    let diContainer: AwilixContainer<Dependencies>
    let publisher: AmqpPermissionPublisher
    beforeAll(async () => {
      logger = new FakeLogger()
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        logger: asFunction(() => logger),
      })
      await diContainer.cradle.permissionConsumer.close()
      publisher = diContainer.cradle.permissionPublisher
    })

    it('logs a message when logging is enabled', async () => {
      const message = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      publisher.publish(message)

      await waitAndRetry(() => {
        return logger.loggedMessages.length === 2
      })

      expect(logger.loggedMessages[1]).toEqual({
        id: '1',
        messageType: 'add',
      })
    })
  })

  describe('init', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
        permissionConsumer: asClass(FakeConsumer, {
          lifetime: Lifetime.SINGLETON,
          asyncInit: 'start',
          asyncDispose: 'close',
          asyncDisposePriority: 10,
        }),
      })
    })

    beforeEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
    })

    afterEach(async () => {
      await channel.deleteQueue(AmqpPermissionConsumer.QUEUE_NAME)
      await channel.close()
    })

    it('throws an error when invalid queue locator is passed', async () => {
      await channel.deleteQueue(AmqpPermissionConsumer.QUEUE_NAME)
      const newPublisher = new AmqpPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueName: AmqpPermissionPublisher.QUEUE_NAME,
        },
      })

      await expect(() => newPublisher.init()).rejects.toThrow(/does not exist/)
    })

    it('does not create a new queue when queue locator is passed', async () => {
      await channel.assertQueue(AmqpPermissionPublisher.QUEUE_NAME)

      const newPublisher = new AmqpPermissionPublisher(diContainer.cradle, {
        locatorConfig: {
          queueName: AmqpPermissionPublisher.QUEUE_NAME,
        },
      })

      await expect(newPublisher.init()).resolves.toBeUndefined()
    })
  })

  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let channel: Channel
    let permissionPublisher: AmqpPermissionPublisher
    let permissionConsumer: AmqpPermissionConsumer

    beforeAll(async () => {
      diContainer = await registerDependencies(TEST_AMQP_CONFIG, {
        consumerErrorResolver: asClass(FakeConsumerErrorResolver, SINGLETON_CONFIG),
      })
      permissionPublisher = diContainer.cradle.permissionPublisher
      permissionConsumer = diContainer.cradle.permissionConsumer
    })

    beforeEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
      await permissionConsumer.start()
    })

    afterEach(async () => {
      const connection = await diContainer.cradle.amqpConnectionManager.getConnection()
      channel = await connection.createChannel()
      await channel.deleteQueue(AmqpPermissionPublisher.QUEUE_NAME)
      await channel.close()
    })

    afterAll(async () => {
      const { awilixManager } = diContainer.cradle
      await awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publish unexpected message', async () => {
      expect.assertions(3)
      try {
        permissionPublisher.publish({
          hello: 'world',
          messageType: 'add',
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } as any)
      } catch (error) {
        expect(error).toBeDefined()
        expect(error).toBeInstanceOf(Error)
        expect(error).toBeInstanceOf(ZodError)
      }
    })

    it('return details if publish failed', async () => {
      expect.assertions(3)
      try {
        await permissionPublisher.close()
        permissionPublisher.publish({
          id: '11',
          messageType: 'add',
        })
      } catch (error) {
        expect(error).toBeInstanceOf(Error)
        expect((error as InternalError).message).toMatchInlineSnapshot(
          `"Error while publishing to AMQP Cannot read properties of undefined (reading 'sendToQueue')"`,
        )
        expect((error as InternalError).details).toMatchInlineSnapshot(`
          {
            "messageType": "add",
            "publisher": "AmqpPermissionPublisher",
            "queueName": "user_permissions_multi",
          }
        `)
      }
      await diContainer.cradle.amqpConnectionManager.reconnect()
    })

    it('publish message with unsupported message type', async () => {
      let error: unknown
      try {
        permissionPublisher.publish({
          id: '124',
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          messageType: 'bad' as any,
        })
      } catch (e) {
        error = e
      }
      console.log(error)
      expect(error).toBeDefined()
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toBe('Unsupported message type: bad')
    })

    it('publishes a message', async () => {
      await permissionConsumer.close()

      const message = {
        id: '1',
        messageType: 'add',
        userIds: [1],
        permissions: ['100'],
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_MESSAGE_TYPE

      let receivedMessage: unknown
      await channel.consume(AmqpPermissionPublisher.QUEUE_NAME, (message) => {
        if (message === null) {
          return
        }
        const decodedMessage = deserializeAmqpMessage(
          message,
          PERMISSIONS_MESSAGE_SCHEMA,
          new FakeConsumerErrorResolver(),
        )
        receivedMessage = decodedMessage.result!
      })

      permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)

      expect(receivedMessage).toEqual({
        parsedMessage: message,
        originalMessage: message,
      })
    })

    it('publishes a message auto-filling timestamp', async () => {
      await permissionConsumer.close()

      const message = {
        id: '2',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      let receivedMessage: unknown
      await channel.consume(AmqpPermissionPublisher.QUEUE_NAME, (message) => {
        if (message === null) {
          return
        }
        const decodedMessage = deserializeAmqpMessage(
          message,
          PERMISSIONS_ADD_MESSAGE_SCHEMA,
          new FakeConsumerErrorResolver(),
        )
        receivedMessage = decodedMessage.result!
      })

      permissionPublisher.publish(message)

      await waitAndRetry(() => !!receivedMessage)

      expect(receivedMessage).toEqual({
        parsedMessage: {
          id: '2',
          messageType: 'add',
        },
        originalMessage: {
          id: '2',
          messageType: 'add',
          timestamp: expect.any(String),
        },
      })
    })

    it('reconnects on lost connection', async () => {
      const message = {
        id: '4',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await diContainer.cradle.amqpConnectionManager.getConnectionSync()!.close()

      const updatedUsersPermissions = await waitAndRetry(
        () => {
          permissionPublisher.publish(message)

          return permissionConsumer.addCounter > 0
        },
        100,
        20,
      )

      if (null === updatedUsersPermissions) {
        throw new Error('Users permissions unexpectedly null')
      }

      expect(permissionConsumer.addCounter).toBeGreaterThan(0)
    })
  })
})
