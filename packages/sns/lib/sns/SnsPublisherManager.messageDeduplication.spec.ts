import {
  CommonMetadataFiller,
  type MessageDeduplicationKeyGenerator,
} from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { type AwilixContainer, asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { TestEventDeduplicationKeyGenerator } from '../../test/publishers/TestEventDeduplicationKeyGenerator'
import { cleanRedis } from '../../test/utils/cleanRedis'
import type {
  Dependencies,
  TestEventPublishPayloadsType,
  TestEventsType,
} from '../../test/utils/testContext'
import { TestEvents, registerDependencies } from '../../test/utils/testContext'
import { type CommonSnsPublisher, CommonSnsPublisherFactory } from './CommonSnsPublisherFactory'
import { SnsPublisherManager } from './SnsPublisherManager'

const TEST_DEDUPLICATION_KEY_PREFIX = 'test_key_prefix'

describe('SnsPublisherManager', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >
  let messageDeduplicationStore: RedisMessageDeduplicationStore
  let messageDeduplicationKeyGenerator: MessageDeduplicationKeyGenerator

  beforeAll(async () => {
    diContainer = await registerDependencies(
      {
        publisherManager: asValue(() => undefined),
      },
      false,
    )
    messageDeduplicationStore = new RedisMessageDeduplicationStore(
      {
        redis: diContainer.cradle.redis,
      },
      { keyPrefix: TEST_DEDUPLICATION_KEY_PREFIX },
    )
    messageDeduplicationKeyGenerator = new TestEventDeduplicationKeyGenerator()
  })

  beforeEach(() => {
    publisherManager = new SnsPublisherManager(diContainer.cradle, {
      metadataFiller: new CommonMetadataFiller({
        serviceId: 'service',
      }),
      publisherFactory: new CommonSnsPublisherFactory(),
      newPublisherOptions: {
        handlerSpy: true,
        messageIdField: 'id',
        messageTypeField: 'type',
        creationConfig: {
          updateAttributesIfExists: true,
        },
        messageDeduplicationConfig: {
          deduplicationStore: messageDeduplicationStore,
          messageTypeToConfigMap: {
            'entity.created': {
              deduplicationKeyGenerator: messageDeduplicationKeyGenerator,
              deduplicationWindowSeconds: 10,
            },
            // 'entity.update' is not configured on purpose
          },
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

  describe('publish', () => {
    it('publishes a message and writes deduplication key to store when message type is configured with deduplication', async () => {
      const message = {
        payload: {
          entityId: '1',
          newData: 'msg',
        },
        type: 'entity.created',
      } satisfies TestEventPublishPayloadsType

      const publishedMessage = await publisherManager.publish(TestEvents.created.snsTopic, message)

      const spy = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessage.id)
      expect(spy.processingResult).toBe('published')

      const deduplicationKey = messageDeduplicationStore.retrieveKey(
        messageDeduplicationKeyGenerator.generate(message),
      )
      expect(deduplicationKey).not.toBeNull()
    })

    it('does not publish the same message if deduplication key already exists', async () => {
      const message = {
        payload: {
          entityId: '1',
          newData: 'msg',
        },
        type: 'entity.created',
      } satisfies TestEventPublishPayloadsType

      // Message is published for the initial call
      const publishedMessageFirstCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message,
      )

      const spyFirstCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageFirstCall.id)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message is not published for the subsequent call
      const publishedMessageSecondCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message,
      )

      const spySecondCall = publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .checkForMessage({
          id: publishedMessageSecondCall.id,
        })
      expect(spySecondCall).toBeUndefined()
    })

    it('works only for event types that are configured', async () => {
      const message1 = {
        payload: {
          entityId: '1',
          newData: 'msg',
        },
        type: 'entity.created',
      } satisfies TestEventPublishPayloadsType
      const message2 = {
        payload: {
          entityId: '1',
          updatedData: 'msg',
        },
        type: 'entity.updated',
      } satisfies TestEventPublishPayloadsType

      // Message 1 is published for the initial call
      const publishedMessageFirstCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message1,
      )

      const spyFirstCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageFirstCall.id)
      expect(spyFirstCall.processingResult).toBe('published')

      // Clear the spy, so wew can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message 1 is not published for the subsequent call (deduplication works)
      const publishedMessageSecondCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message1,
      )

      const spySecondCall = publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .checkForMessage({
          id: publishedMessageSecondCall.id,
        })
      expect(spySecondCall).toBeUndefined()

      // Clear the spy, so we can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message 2 is published for the initial call
      const publishedMessageThirdCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message2,
      )

      const spyThirdCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageThirdCall.id)
      expect(spyThirdCall.processingResult).toBe('published')

      // Clear the spy, so we can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message 2 is published for the subsequent call (deduplication does not work)
      const publishedMessageFourthCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message2,
      )

      const spyFourthCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageFourthCall.id)
      expect(spyFourthCall.processingResult).toBe('published')
    })
  })
})
