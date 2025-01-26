import type {
  ConsumerMessageDeduplicationMessageTypeConfig,
  ConsumerMessageDeduplicationStore,
  MessageDeduplicationConfig,
  MessageDeduplicationKeyGenerator,
} from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import type { SqsPermissionPublisher } from '../publishers/SqsPermissionPublisher'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import { setTimeout } from 'node:timers/promises'
import { waitAndRetry } from '@lokalise/node-core'
import { ConsumerMessageDeduplicationKeyStatus } from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { PermissionMessageDeduplicationKeyGenerator } from '../utils/PermissionMessageDeduplicationKeyGenerator'
import { cleanRedis } from '../utils/cleanRedis'
import { SqsPermissionConsumer } from './SqsPermissionConsumer'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'

const TEST_DEDUPLICATION_KEY_PREFIX = 'test_key_prefix'

type ConsumerMessageDeduplicationConfig = MessageDeduplicationConfig<
  ConsumerMessageDeduplicationStore,
  ConsumerMessageDeduplicationMessageTypeConfig
>

describe('SqsPermissionConsumer', () => {
  let diContainer: AwilixContainer<Dependencies>
  let messageDeduplicationStore: RedisMessageDeduplicationStore
  let messageDeduplicationKeyGenerator: MessageDeduplicationKeyGenerator
  let consumerMessageDeduplicationConfig: ConsumerMessageDeduplicationConfig
  let publisher: SqsPermissionPublisher

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionConsumer: asValue(() => undefined),
    })
    publisher = diContainer.cradle.permissionPublisher
    messageDeduplicationStore = new RedisMessageDeduplicationStore(
      {
        redis: diContainer.cradle.redis,
      },
      { keyPrefix: TEST_DEDUPLICATION_KEY_PREFIX },
    )
    messageDeduplicationKeyGenerator = new PermissionMessageDeduplicationKeyGenerator()
    consumerMessageDeduplicationConfig = {
      deduplicationStore: messageDeduplicationStore,
      messageTypeToConfigMap: {
        add: {
          deduplicationKeyGenerator: messageDeduplicationKeyGenerator,
          deduplicationWindowSeconds: 30,
          maximumProcessingTimeSeconds: 10,
        },
        // 'remove' not configured on purpose
      },
    }
  })

  beforeEach(async () => {
    await publisher.init()
  })

  afterEach(async () => {
    await cleanRedis(diContainer.cradle.redis)
    await publisher.close()
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('consume', () => {
    describe('message type with deduplication configured', () => {
      it('does not consume message with the same deduplication key twice', async () => {
        const consumer = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig,
        })
        await consumer.start()

        const message = {
          id: '1',
          messageType: 'add',
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
          consumerMessageDeduplicationConfig,
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

        const message = {
          id: '1',
          messageType: 'add',
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

        // Ensure deduplication key exists and has correct TTL to prevent the same message from being processed again within the deduplication window
        const deduplicationKey = messageDeduplicationKeyGenerator.generate(message)
        const deduplicationValue = await messageDeduplicationStore.getByKey(deduplicationKey)
        expect(deduplicationValue).toBe(ConsumerMessageDeduplicationKeyStatus.PROCESSED)
        const deduplicationKeyTtl = await messageDeduplicationStore.getKeyTtl(deduplicationKey)
        expect(deduplicationKeyTtl).toBeGreaterThan(
          consumerMessageDeduplicationConfig.messageTypeToConfigMap.add.deduplicationWindowSeconds -
            5,
        ) // -5 to account for the time it took to process the message

        await consumer.close(true)
      })

      it('message is processable again after deduplication window has expired', async () => {
        const consumer = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig,
        })
        await consumer.start()

        const message = {
          id: '1',
          messageType: 'add',
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
        await messageDeduplicationStore.deleteKey(
          messageDeduplicationKeyGenerator.generate(message),
        )

        // Clear the spy, so we can check subsequent call
        consumer.handlerSpy.clear()

        await publisher.publish(message)

        // Message is successfully processed after deduplication key has expired
        const thirdConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
        expect(thirdConsumptionResult.processingResult).toBe('consumed')

        await consumer.close()
      })

      it('another consumer takes over the message processing in case the first consumer takes too long to process the message (i.e. exceeds the maximum processing time)', async () => {
        const consumerMessageDeduplicationConfigOverrides = {
          ...consumerMessageDeduplicationConfig,
          messageTypeToConfigMap: {
            add: {
              ...consumerMessageDeduplicationConfig.messageTypeToConfigMap.add,
              maximumProcessingTimeSeconds: 2,
            },
          },
        } satisfies ConsumerMessageDeduplicationConfig
        const consumer1 = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig: consumerMessageDeduplicationConfigOverrides,
          addHandlerOverride: async () => {
            await setTimeout(5000)
            return Promise.resolve({ result: 'success' })
          },
        })
        await consumer1.start()
        const consumer2 = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig: consumerMessageDeduplicationConfigOverrides,
        })
        // Not starting consumer 2 yet

        const message = {
          id: '1',
          messageType: 'add',
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

        await publisher.publish(message)

        // Wait until consumer 1 acquires lock and then simulate its fatal failure by force disposing it
        const deduplicationKey = messageDeduplicationKeyGenerator.generate(message)
        await waitAndRetry(async () => {
          const key = await messageDeduplicationStore.getByKey(deduplicationKey)

          return key !== null
        })
        await consumer1.close(true)

        // Start consumer 2 and publish the same message again
        await consumer2.start()
        await publisher.publish(message)

        // Consumer 2 cannot take over the processing at this point - it has to wait for the maximum processing time to pass
        const secondConsumptionResult = await consumer2.handlerSpy.waitForMessageWithId(message.id)
        expect(secondConsumptionResult.processingResult).toBe('duplicate')

        // Clear the spy, so we can check subsequent call
        consumer2.handlerSpy.clear()

        // Consumer 2 has taken over the message processing
        const thirdConsumptionResult = await consumer2.handlerSpy.waitForMessageWithId(
          message.id,
          'consumed',
        )
        expect(thirdConsumptionResult.message).toMatchObject(message)

        await consumer2.close()
      })

      it('consumes messages with different deduplication keys', async () => {
        const consumer = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig,
        })
        await consumer.start()

        const message1 = {
          id: '1',
          messageType: 'add',
        } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
        const message2 = {
          id: '2',
          messageType: 'add',
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
    })

    describe('message type without deduplication configured', () => {
      it('consumes the same message twice', async () => {
        const consumer = new SqsPermissionConsumer(diContainer.cradle, {
          consumerMessageDeduplicationConfig,
        })
        await consumer.start()

        const message = {
          id: '1',
          messageType: 'remove',
        } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE

        await publisher.publish(message)

        // Message is successfully processed during the first consumption
        const firstConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
        expect(firstConsumptionResult.processingResult).toBe('consumed')

        // Clear the spy, so we can check subsequent call
        consumer.handlerSpy.clear()

        await publisher.publish(message)

        // Message is successfully processed during the second consumption
        const secondConsumptionResult = await consumer.handlerSpy.waitForMessageWithId(message.id)
        expect(secondConsumptionResult.processingResult).toBe('consumed')

        await consumer.close()
      })
    })
  })

  describe('init', () => {
    it('throws error if invalid deduplication config provided', () => {
      expect(
        () =>
          new SqsPermissionConsumer(diContainer.cradle, {
            consumerMessageDeduplicationConfig: {
              deduplicationStore: messageDeduplicationStore,
              messageTypeToConfigMap: {
                add: {
                  deduplicationKeyGenerator: messageDeduplicationKeyGenerator,
                  maximumProcessingTimeSeconds: -1,
                  deduplicationWindowSeconds: -1,
                },
              },
            },
          }),
      ).toThrowError(/Invalid consumer message deduplication config provided/)
    })
  })
})
