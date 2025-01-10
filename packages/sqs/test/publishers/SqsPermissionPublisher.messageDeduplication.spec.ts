import { type AwilixContainer, asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'

import type { MessageDeduplicationKeyGenerator } from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { cleanRedis } from '../utils/cleanRedis'
import { PermissionMessageDeduplicationKeyGenerator } from './PermissionMessageDeduplicationKeyGenerator'
import { SqsPermissionPublisher } from './SqsPermissionPublisher'

const TEST_DEDUPLICATION_KEY_PREFIX = 'test_key_prefix'

describe('SqsPermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisher
    let messageDeduplicationStore: RedisMessageDeduplicationStore
    let messageDeduplicationKeyGenerator: MessageDeduplicationKeyGenerator

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      messageDeduplicationStore = new RedisMessageDeduplicationStore(
        {
          redis: diContainer.cradle.redis,
        },
        { keyPrefix: TEST_DEDUPLICATION_KEY_PREFIX },
      )
      messageDeduplicationKeyGenerator = new PermissionMessageDeduplicationKeyGenerator()
    })

    beforeEach(() => {
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        messageDeduplicationConfig: {
          deduplicationWindowSeconds: 10,
          deduplicationKeyGenerator: messageDeduplicationKeyGenerator,
          deduplicationStore: messageDeduplicationStore,
        },
      })
    })

    afterEach(async () => {
      await cleanRedis(diContainer.cradle.redis)
      await publisher.close()
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('writes deduplication key to store using provided deduplication function and publishes message', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spy = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spy.message).toEqual(message)
      expect(spy.processingResult).toBe('published')

      const deduplicationKey = await messageDeduplicationStore.retrieveKey(
        messageDeduplicationKeyGenerator.generate(message),
      )
      expect(deduplicationKey).not.toBeNull()
    })

    it('does not publish the same message if deduplication key already exists', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      // Message is published for the initial call
      await publisher.publish(message)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spyFirstCall.message).toEqual(message)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message is not published for the subsequent call
      await publisher.publish(message)

      const spySecondCall = publisher.handlerSpy.checkForMessage({
        messageType: 'add',
      })
      expect(spySecondCall).toBeUndefined()
    })

    it('publishing messages that produce different deduplication keys does not affect each other', async () => {
      const message1 = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: '2',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE

      // Message 1 is published
      await publisher.publish(message1)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(spyFirstCall.message).toEqual(message1)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published
      await publisher.publish(message2)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId('2', 'published')
      expect(spySecondCall.message).toEqual(message2)
      expect(spySecondCall.processingResult).toBe('published')
    })
  })
})
