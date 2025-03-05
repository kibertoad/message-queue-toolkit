import { setTimeout } from 'node:timers/promises'
import type { MessageDeduplicationConfig } from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import { cleanRedis } from '../utils/cleanRedis'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'
import { SqsPermissionConsumer } from './SqsPermissionConsumer'

import { waitAndRetry } from '@lokalise/node-core'
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
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Message is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is not processed due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toEqual({
        status: 'consumed',
        skippedAsDuplicate: true,
      })

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
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish({
        ...message,
        metadata: {
          forceConsumerToThrow: true,
        },
      })

      // First message processing fails
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toEqual({
        status: 'error',
        errorReason: 'handlerError',
      })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Second one is processed immediately because deduplication key is cleared after the failed attempt
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toEqual({ status: 'consumed' })

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
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Message is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(firstConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is not processed due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(secondConsumptionResult.processingResult).toEqual({
        status: 'consumed',
        skippedAsDuplicate: true,
      })

      // We're expiring the deduplication key, so we do not have to wait for the deduplication window to pass
      await messageDeduplicationStore.deleteKey(`consumer:${deduplicationId}`)

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message)

      // Message is successfully processed after deduplication key has expired
      const thirdConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(thirdConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      await consumer.close()
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
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: '2',
        messageType: 'add',
        deduplicationId: '2',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message1)

      // Message 1 is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(firstConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the second consumption
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(secondConsumptionResult.processingResult).toEqual({ status: 'consumed' })

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
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      vi.spyOn(messageDeduplicationStore, 'acquireLock').mockResolvedValue({
        error: new Error('Test error'),
      })

      await publisher.publish(message)

      const spy = await consumer.handlerSpy.waitForMessageWithId('1')
      expect(spy.processingResult).toEqual({ status: 'consumed' })

      await consumer.close()
    })

    it('works only for messages that have deduplication ids provided', async () => {
      const consumer = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      await consumer.start()

      const message1 = {
        id: '1',
        messageType: 'remove',
        deduplicationId: '1',
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE
      const message2 = {
        id: '1',
        messageType: 'add',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message1)

      // Message 1 is successfully processed during the first consumption
      const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(firstConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message1)

      // Message 1 is not processed again due to deduplication
      const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message1.id)
      expect(secondConsumptionResult.processingResult).toEqual({
        status: 'consumed',
        skippedAsDuplicate: true,
      })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the first consumption
      const thirdConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(thirdConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      // Clear the spy, so we can check subsequent call
      consumer.handlerSpy.clear()

      await publisher.publish(message2)

      // Message 2 is successfully processed during the second consumption (deduplication is not applied)
      const fourthConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message2.id)
      expect(fourthConsumptionResult.processingResult).toEqual({ status: 'consumed' })

      await consumer.close()
    })

    it('passes custom deduplication options to the deduplication store', async () => {
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
        deduplicationOptions: {
          deduplicationWindowSeconds: 1000,
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spy = await consumer.handlerSpy.waitForMessageWithId(message.id)
      expect(spy.processingResult).toEqual({ status: 'consumed' })

      const deduplicationKeyTtl = await messageDeduplicationStore.getKeyTtl(
        `consumer:${deduplicationId}`,
      )
      expect(deduplicationKeyTtl).toBeGreaterThanOrEqual(1000 - 5)

      await consumer.close()
    })

    it('if lock cannot be acquired within acceptable time, message is enqueued', async () => {
      const consumer1 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
        addHandlerOverride: async () => {
          await setTimeout(2500)
          return Promise.resolve({ result: 'success' })
        },
      })
      await consumer1.start()
      const consumer2 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      // Not starting consumer2 yet, so consumer1 can acquire the lock first

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        deduplicationOptions: {
          acquireTimeoutSeconds: 1, // consumer2 will wait for lock 1 second, while consumer1 needs at least 2.5 seconds to release the lock
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Wait until consumer1 acquires the lock
      await waitAndRetry(async () => {
        return await messageDeduplicationStore.keyExists(`mutex:consumer:${deduplicationId}`)
      })

      await consumer2.start()

      // Publish the message again, so consumer2 can pick it up
      await publisher.publish(message)

      const consumer2Spy = await consumer2.handlerSpy.waitForMessageWithId(message.id)
      expect(consumer2Spy.processingResult).toEqual({ status: 'retryLater' })

      // Wait until consumer1 releases the lock, to omit errors related to the lock being lost
      const consumer1Spy = await consumer1.handlerSpy.waitForMessageWithId(message.id)
      expect(consumer1Spy.processingResult).toEqual({ status: 'consumed' })

      await Promise.all([consumer1.close(), consumer2.close()])
    })

    it('respects deduplication key even after lock is released', async () => {
      const consumer1 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
        addHandlerOverride: async () => {
          await setTimeout(3000)
          return Promise.resolve({ result: 'success' })
        },
      })
      await consumer1.start()
      const consumer2 = new SqsPermissionConsumer(diContainer.cradle, {
        messageDeduplicationConfig,
        enableConsumerDeduplication: true,
      })
      // Not starting consumer2 yet, so consumer1 can acquire the lock first

      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
        /**
         * Consumer #1 needs at least 3 seconds to process the message.
         * With the below values, consumer #2 should start acquiring the lock
         * while consumer #2 is still processing. The lock will be ultimately
         * acquired by consumer #2 once consumer #1 releases it and marks
         * the message as processed.
         */
        deduplicationOptions: {
          lockTimeoutSeconds: 5,
          acquireTimeoutSeconds: 5,
          deduplicationWindowSeconds: 10,
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      // Wait until consumer1 acquires the lock
      await waitAndRetry(async () => {
        return await messageDeduplicationStore.keyExists(`mutex:consumer:${deduplicationId}`)
      })

      await consumer2.start()

      await publisher.publish(message)

      // consumer1 processes the message
      const consumer1Spy = await consumer1.handlerSpy.waitForMessageWithId(message.id)
      expect(consumer1Spy.processingResult).toEqual({ status: 'consumed' })

      // consumer2 reports the message as duplicate
      const consumer2Spy = await consumer2.handlerSpy.waitForMessageWithId(message.id)
      expect(consumer2Spy.processingResult).toEqual({
        status: 'consumed',
        skippedAsDuplicate: true,
      })

      await Promise.all([consumer1.close(), consumer2.close()])
    }, 7000)
  })
})
