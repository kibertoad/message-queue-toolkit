import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { waitAndRetry } from '@lokalise/node-core'
import type { MessageDeduplicationConfig } from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { cleanRedis } from '../utils/cleanRedis'
import { SqsPermissionConsumer } from './SqsPermissionConsumer'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'

describe('SqsPermissionConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let messageDeduplicationStore: RedisMessageDeduplicationStore
  let messageDeduplicationConfig: MessageDeduplicationConfig
  let publisher: SqsPermissionPublisher

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionConsumer: asValue(() => undefined),
    })
    publisher = diContainer.cradle.permissionPublisher
    messageDeduplicationStore = new RedisMessageDeduplicationStore({
      redis: diContainer.cradle.redis,
    })
    messageDeduplicationConfig = {
      deduplicationStore: messageDeduplicationStore,
    }
  })

  beforeEach(async () => {
    await publisher.init()
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await cleanRedis(diContainer.cradle.redis)
    await publisher.close()
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('consume', () => {
    it('does not consume message with the same deduplication id twice', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId: '1',
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Message is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toBe('consumed')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is not processed due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toBe('duplicate')

      await consumer.close()
    })

    it('consumes second message immediately when the first one failed to be processed', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
        consumerOverrides: {
          terminateVisibilityTimeout: false, // Setting it to false to let consumer process the next message rather than keep retrying the first one indefinitely
        },
        addHandlerOverride: (message) => {
          if ((message as PERMISSIONS_ADD_MESSAGE_TYPE)?.metadata?.forceConsumerToThrow) {
            throw new Error('Forced error')
          }
          return Promise.resolve({ result: 'success' })
        },
      })
      await consumer.start()

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish({
        ...message,
        metadata: {
          forceConsumerToThrow: true,
        },
      })

      // First message processing fails
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toBe('error')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Second one is processed immediately because deduplication key is cleared after the failed attempt
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toBe('consumed')

      await consumer.close(true)
    })

    it('message is processable again after deduplication window has expired', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Message is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toBe('consumed')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is not processed due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toBe('duplicate')

      // We're expiring the deduplication key, so we do not have to wait for the deduplication window to pass
      await messageDeduplicationStore.deleteKey(`consumer:${deduplicationId}`)

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is successfully processed after deduplication key has expired
      const thirdConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(thirdConsumptionResult.processingResult).toBe('consumed')

      await consumer.close()
    })

    it('another consumer takes over the message processing in case the first consumer dies', async () => {
      const consumer1 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
        addHandlerOverride: async () => {
          await setTimeout(2000)
          return Promise.resolve({ result: 'success' })
        },
      })
      await consumer1.start()
      const consumer2 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
      })
      // Not starting consumer 2 yet

      const deduplicationId = randomUUID()
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Wait until consumer 1 acquires lock and then simulate its fatal failure by force disposing it
      await waitAndRetry(async () => {
        return await messageDeduplicationStore.keyExists(`mutex:consumer:${deduplicationId}`)
      })
      await consumer1.close(true)

      // Start consumer 2 and publish the same message again
      await consumer2.start()
      await publisher.publish(message)

      // Consumer 2 has taken over the message processing
      const spy = await consumer2.handlerSpy.waitForMessageWithId(message.id, 'consumed')
      expect(spy.message).toMatchObject(message)

      await consumer2.close()
    })

    it('consumes messages with different deduplication ids', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const message1 = {
        id: '1',
        messageType: 'add',
        deduplicationId: '1',
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: '2',
        messageType: 'add',
        deduplicationId: '2',
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message1)

      // Message 1 is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(firstConsumptionResult.processingResult).toBe('consumed')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the second consumption
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(secondConsumptionResult.processingResult).toBe('consumed')

      await consumer.close()
    })

    it('in case of errors on deduplication store level, message is consumed', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      vi.spyOn(messageDeduplicationStore, 'acquireLock').mockResolvedValue({
        error: new Error('Test error'),
      })

      await publisher.publish(message)

      const spy = await consumer.handlerSpy.waitForMessageWithId('1')
      expect(spy.processingResult).toBe('consumed')

      await consumer.close()
    })

    it('works only for messages that have deduplication details provided', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const message1 = {
        id: '1',
        messageType: 'remove',
        deduplicationId: '1',
        deduplicationWindowSeconds: 10,
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE
      const message2 = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message1)

      // Message 1 is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(firstConsumptionResult.processingResult).toBe('consumed')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message1)

      // Message 1 is not processed again due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(secondConsumptionResult.processingResult).toBe('duplicate')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the first consumption
      const thirdConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(thirdConsumptionResult.processingResult).toBe('consumed')

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the second consumption (deduplication is not applied)
      const fourthConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(fourthConsumptionResult.processingResult).toBe('consumed')

      await consumer.close()
    })

    it('if deduplication window seconds is not provided, it uses default value and deduplicates the message', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spy = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(spy.processingResult).toBe('consumed')

      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `consumer:${deduplicationId}`,
      )
      expect(deduplicationKeyExists).toBe(true)

      await consumer.close()
    })
  })
})
