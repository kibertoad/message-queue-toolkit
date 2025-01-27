import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { type AwilixContainer, asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import { cleanRedis } from '../utils/cleanRedis'
import type { Dependencies } from '../utils/testContext'
import { registerDependencies } from '../utils/testContext'
import { SnsPermissionPublisher } from './SnsPermissionPublisher'

const TEST_DEDUPLICATION_KEY_PREFIX = 'test_key_prefix'

describe('SnsPermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SnsPermissionPublisher
    let messageDeduplicationStore: RedisMessageDeduplicationStore

    beforeAll(async () => {
      diContainer = await registerDependencies(
        {
          permissionPublisher: asValue(() => undefined),
        },
        false,
      )
      messageDeduplicationStore = new RedisMessageDeduplicationStore(
        {
          redis: diContainer.cradle.redis,
        },
        { keyPrefix: TEST_DEDUPLICATION_KEY_PREFIX },
      )
    })

    beforeEach(() => {
      publisher = new SnsPermissionPublisher(diContainer.cradle, {
        publisherMessageDeduplicationConfig: {
          deduplicationStore: messageDeduplicationStore,
          messageTypeToConfigMap: {
            add: {
              deduplicationWindowSeconds: 10,
            },
            // 'remove' is not configured on purpose
          },
        },
      })
    })

    afterEach(async () => {
      await cleanRedis(diContainer.cradle.redis)
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes a message and writes deduplication key to store when message type is configured with deduplication', async () => {
      const deduplicationId = '1'
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spy = await publisher.handlerSpy.waitForMessageWithId(message.id)
      expect(spy.processingResult).toBe('published')

      const deduplicationKeyValue = await messageDeduplicationStore.getByKey(deduplicationId)
      expect(deduplicationKeyValue).not.toBeNull()
    })

    it('does not publish the same message if deduplication key already exists', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        deduplicationId: '1',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      // Message is published for the initial call
      await publisher.publish(message)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId(message.id)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message is not published for the subsequent call
      await publisher.publish(message)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId(message.id)
      expect(spySecondCall.processingResult).toBe('duplicate')
    })

    it('works only for event types that are configured', async () => {
      const message1 = {
        id: '1',
        messageType: 'add',
        deduplicationId: '1',
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: '1',
        messageType: 'remove',
        deduplicationId: '1', // Even though it's set, the message type is not configured on a publisher level - deduplication should not work
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE

      // Message 1 is published for the initial call
      await publisher.publish(message1)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId(message1.id)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so wew can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 1 is not published for the subsequent call (deduplication works)
      await publisher.publish(message1)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId(message1.id)
      expect(spySecondCall.processingResult).toBe('duplicate')

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published for the initial call
      await publisher.publish(message2)

      const spyThirdCall = await publisher.handlerSpy.waitForMessageWithId(message2.id)
      expect(spyThirdCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published for the subsequent call (deduplication does not work)
      await publisher.publish(message2)

      const spyFourthCall = await publisher.handlerSpy.waitForMessageWithId(message2.id)
      expect(spyFourthCall.processingResult).toBe('published')
    })
  })
})
