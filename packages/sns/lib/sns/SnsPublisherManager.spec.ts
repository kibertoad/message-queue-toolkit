import { randomUUID } from 'node:crypto'

import { BASE_MESSAGE_SCHEMA } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import z from 'zod'

import type {
  Dependencies,
  TestEventPayloadsType,
  TestEventsType,
} from '../../test/utils/testContext'
import { registerDependencies, TestEvents } from '../../test/utils/testContext'

import { CommonSnsPublisher } from './CommonSnsPublisherFactory'
import type { SnsMessagePublishType, SnsPublisherManager } from './SnsPublisherManager'
import { FakeConsumer } from './fakes/FakeConsumer'

describe('SnsPublisherManager', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPayloadsType>,
    TestEventsType
  >

  beforeAll(async () => {
    diContainer = await registerDependencies()
    publisherManager = diContainer.cradle.publisherManager
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('publish', () => {
    it('publishes to a correct publisher', async () => {
      // Given
      const message = {
        payload: {
          message: 'msg',
        },
        type: 'entity.created',
      } satisfies SnsMessagePublishType<typeof TestEvents.created>

      const fakeConsumer = new FakeConsumer(
        diContainer.cradle,
        'queue',
        TestEvents.created.snsTopic,
        TestEvents.created.schema,
      )
      await fakeConsumer.start()

      // When
      const publishedMessage = await publisherManager.publish(TestEvents.created.snsTopic, message)

      const handlerSpyPromise = publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessage.id)

      const consumerResult = await fakeConsumer.handlerSpy.waitForMessageWithId(publishedMessage.id)
      const publishedMessageResult = await handlerSpyPromise

      expect(consumerResult.processingResult).toBe('consumed')
      expect(publishedMessageResult.processingResult).toBe('published')

      expect(consumerResult.message).toMatchObject({
        id: publishedMessage.id,
        metadata: {
          correlationId: expect.any(String),
          originatedFrom: 'service',
          producedBy: 'service',
          schemaVersion: '1.0.1',
        },
        payload: {
          message: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })

      await fakeConsumer.close()
    })

    it('publish to a non-existing topic will throw error', async () => {
      await expect(
        publisherManager.publish('non-existing-topic', {
          type: 'entity.created',
          payload: {
            message: 'msg',
          },
        }),
      ).rejects.toThrow('No publisher for topic non-existing-topic')
    })
  })

  describe('handlerSpy', () => {
    it('returns correct handler spy', () => {
      const spy = publisherManager.handlerSpy(TestEvents.created.snsTopic)
      expect(spy).toBeDefined()
    })

    it('returns error when no publisher for topic', () => {
      expect(() => publisherManager.handlerSpy('non-existing-topic')).toThrow(
        'No publisher for topic non-existing-topic',
      )
    })
  })

  describe('injectPublisher', () => {
    it('works correctly', async () => {
      // Given
      const topic = 'test-topic'
      const newPublisher = new CommonSnsPublisher(diContainer.cradle, {
        creationConfig: {
          topic: {
            Name: topic,
          },
        },
        handlerSpy: true,
        messageTypeField: 'type',
        messageSchemas: [TestEvents.created.schema],
      })

      // When
      const messageId = randomUUID()
      // @ts-ignore
      publisherManager.injectPublisher(topic, newPublisher)
      publisherManager.injectEventDefinition({
        schema: BASE_MESSAGE_SCHEMA.extend({
          type: z.literal('entity.created'),
        }),
        snsTopic: topic,
        schemaVersion: '2.0.0',
      })

      await publisherManager.publish(topic, {
        id: messageId,
        type: 'entity.created',
        payload: {
          message: 'msg',
        },
      })

      // Then
      const spyRes = await publisherManager
        .handlerSpy(topic)
        .waitForMessageWithId(messageId, 'published')
      expect(spyRes.processingResult).toBe('published')
    })
  })
})
