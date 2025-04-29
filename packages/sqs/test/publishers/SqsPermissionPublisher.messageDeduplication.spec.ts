import { type AwilixContainer, asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

import { randomUUID } from 'node:crypto'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { cleanRedis } from '../utils/cleanRedis.ts'
import { SqsPermissionPublisher } from './SqsPermissionPublisher.ts'

describe('SqsPermissionPublisher', () => {
  describe('publish', () => {
    let diContainer: AwilixContainer<Dependencies>
    let publisher: SqsPermissionPublisher
    let messageDeduplicationStore: RedisMessageDeduplicationStore

    beforeAll(async () => {
      diContainer = await registerDependencies({
        permissionPublisher: asValue(() => undefined),
        permissionConsumer: asValue(() => undefined),
      })
      messageDeduplicationStore = new RedisMessageDeduplicationStore({
        redis: diContainer.cradle.redis,
      })
    })

    beforeEach(() => {
      publisher = new SqsPermissionPublisher(diContainer.cradle, {
        messageDeduplicationConfig: {
          deduplicationStore: messageDeduplicationStore,
        },
        enablePublisherDeduplication: true,
      })
    })

    afterEach(async () => {
      vi.restoreAllMocks()
      await cleanRedis(diContainer.cradle.redis)
      await publisher.close()
    })

    afterAll(async () => {
      await diContainer.cradle.awilixManager.executeDispose()
      await diContainer.dispose()
    })

    it('publishes a message and stores deduplication id when message contains deduplication id', async () => {
      const deduplicationId = randomUUID()
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId,
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spy = await publisher.handlerSpy.waitForMessageWithId('1')
      expect(spy.processingResult).toEqual({ status: 'published' })

      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `publisher:${deduplicationId}`,
      )
      expect(deduplicationKeyExists).toEqual(true)
    })

    it('does not publish the same message if deduplication id already exists', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId: randomUUID(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      // Message is published for the initial call
      await publisher.publish(message)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('1')
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message is not published for the subsequent call
      await publisher.publish(message)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId('1')
      expect(spySecondCall.processingResult).toEqual({
        status: 'published',
        skippedAsDuplicate: true,
      })
    })

    it('publishing messages with different deduplication ids does not affect each other', async () => {
      const message1 = {
        id: 'id',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId: randomUUID(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: 'id',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
        deduplicationId: randomUUID(),
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE

      // Message 1 is published
      await publisher.publish(message1)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published
      await publisher.publish(message2)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spySecondCall.processingResult).toEqual({ status: 'published' })
    })

    it('works only for messages that have deduplication ids provided', async () => {
      const message1 = {
        id: 'id',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId: randomUUID(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE
      const message2 = {
        id: 'id',
        messageType: 'remove',
        timestamp: new Date().toISOString(),
      } satisfies PERMISSIONS_REMOVE_MESSAGE_TYPE

      // Message 1 is published for the initial call
      await publisher.publish(message1)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 1 is not published for the subsequent call (deduplication works)
      await publisher.publish(message1)

      const spySecondCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spySecondCall.processingResult).toEqual({
        status: 'published',
        skippedAsDuplicate: true,
      })

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published for the initial call
      await publisher.publish(message2)

      const spyThirdCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spyThirdCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so we can check for the subsequent call
      publisher.handlerSpy.clear()

      // Message 2 is published for the subsequent call (deduplication does not work)
      await publisher.publish(message2)

      const spyFourthCall = await publisher.handlerSpy.waitForMessageWithId('id')
      expect(spyFourthCall.processingResult).toEqual({ status: 'published' })
    })

    it('in case of errors on deduplication store level, message is published without being deduplicated', async () => {
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId: randomUUID(),
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      vi.spyOn(messageDeduplicationStore, 'setIfNotExists').mockRejectedValue(
        new Error('Dummy error'),
      )

      await publisher.publish(message)

      const spy = await publisher.handlerSpy.waitForMessageWithId('1')
      expect(spy.processingResult).toEqual({ status: 'published' })

      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `publisher:${message.deduplicationId}`,
      )
      expect(deduplicationKeyExists).toBe(false)
    })

    it('passes custom deduplication options to the deduplication store', async () => {
      const deduplicationId = randomUUID()
      const message = {
        id: '1',
        messageType: 'add',
        timestamp: new Date().toISOString(),
        deduplicationId,
        deduplicationOptions: {
          deduplicationWindowSeconds: 1000,
        },
      } satisfies PERMISSIONS_ADD_MESSAGE_TYPE

      await publisher.publish(message)

      const spyFirstCall = await publisher.handlerSpy.waitForMessageWithId('1')
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      const deduplicationKeyTtl = await messageDeduplicationStore.getKeyTtl(
        `publisher:${deduplicationId}`,
      )
      expect(deduplicationKeyTtl).toBeGreaterThanOrEqual(1000 - 5)
    })
  })
})
