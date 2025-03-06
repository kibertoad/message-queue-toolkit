import { CommonMetadataFiller } from '@message-queue-toolkit/core'
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'
import { type AwilixContainer, asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { cleanRedis } from '../../test/utils/cleanRedis'
import type {
  Dependencies,
  TestEventPublishPayloadsType,
  TestEventsType,
} from '../../test/utils/testContext'
import { TestEvents, registerDependencies } from '../../test/utils/testContext'
import { type CommonSnsPublisher, CommonSnsPublisherFactory } from './CommonSnsPublisherFactory'
import { SnsPublisherManager } from './SnsPublisherManager'

describe('SnsPublisherManager', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >
  let messageDeduplicationStore: RedisMessageDeduplicationStore

  beforeAll(async () => {
    diContainer = await registerDependencies(
      {
        publisherManager: asValue(() => undefined),
      },
      false,
    )
    messageDeduplicationStore = new RedisMessageDeduplicationStore({
      redis: diContainer.cradle.redis,
    })
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
        messageDeduplicationIdField: 'deduplicationId',
        messageDeduplicationOptionsField: 'deduplicationOptions',
        messageDeduplicationConfig: {
          deduplicationStore: messageDeduplicationStore,
        },
        enablePublisherDeduplication: true,
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
    it('publishes a message and stores deduplication key when message contains deduplication id', async () => {
      const deduplicationId = '1'
      const message = {
        payload: {
          newData: 'msg',
        },
        type: 'entity.created',
        deduplicationId,
      } satisfies TestEventPublishPayloadsType

      const publishedMessage = await publisherManager.publish(TestEvents.created.snsTopic, message)

      const spy = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessage.id)
      expect(spy.processingResult).toEqual({ status: 'published' })

      const deduplicationKeyExists = await messageDeduplicationStore.keyExists(
        `publisher:${deduplicationId}`,
      )
      expect(deduplicationKeyExists).toBe(true)
    })

    it('does not publish the same message if deduplication id already exists', async () => {
      const message = {
        payload: {
          newData: 'msg',
        },
        type: 'entity.created',
        deduplicationId: '1',
      } satisfies TestEventPublishPayloadsType

      // Message is published for the initial call
      const publishedMessageFirstCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message,
      )

      const spyFirstCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageFirstCall.id)
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so we can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message is not published for the subsequent call
      const publishedMessageSecondCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message,
      )

      const spySecondCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageSecondCall.id)
      expect(spySecondCall.processingResult).toEqual({
        status: 'published',
        skippedAsDuplicate: true,
      })
    })

    it('works only for messages that have deduplication ids provided', async () => {
      const message1 = {
        payload: {
          newData: 'msg',
        },
        type: 'entity.created',
        deduplicationId: '1',
      } satisfies TestEventPublishPayloadsType
      const message2 = {
        payload: {
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
      expect(spyFirstCall.processingResult).toEqual({ status: 'published' })

      // Clear the spy, so wew can check for the subsequent call
      publisherManager.handlerSpy(TestEvents.created.snsTopic).clear()

      // Message 1 is not published for the subsequent call (deduplication works)
      const publishedMessageSecondCall = await publisherManager.publish(
        TestEvents.created.snsTopic,
        message1,
      )

      const spySecondCall = await publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessageSecondCall.id)
      expect(spySecondCall.processingResult).toEqual({
        status: 'published',
        skippedAsDuplicate: true,
      })

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
      expect(spyThirdCall.processingResult).toEqual({ status: 'published' })

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
      expect(spyFourthCall.processingResult).toEqual({ status: 'published' })
    })
  })
})
