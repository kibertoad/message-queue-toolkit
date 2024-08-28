import { randomUUID } from 'node:crypto'

import { enrichMessageSchemaWithBase } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import z from 'zod'

import { SnsSqsEntityConsumer } from '../../test/consumers/SnsSqsEntityConsumer'
import type {
  Dependencies,
  TestEventPublishPayloadsType,
  TestEventsType,
} from '../../test/utils/testContext'
import { TestEvents, registerDependencies } from '../../test/utils/testContext'

import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { CommonSnsPublisher } from './CommonSnsPublisherFactory'
import type { SnsPublisherManager } from './SnsPublisherManager'

describe('SnsPublisherManager', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisherManager: SnsPublisherManager<
    CommonSnsPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >

  beforeAll(async () => {
    diContainer = await registerDependencies({}, false)
    publisherManager = diContainer.cradle.publisherManager
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('publish', () => {
    it('publishes to a correct publisher', async () => {
      // Given
      const consumer = new SnsSqsEntityConsumer(diContainer.cradle)
      await consumer.start()

      // When
      const publishedMessage = await publisherManager.publish(TestEvents.created.snsTopic, {
        payload: {
          newData: 'msg',
        },
        type: 'entity.created',
      })

      const handlerSpyPromise = publisherManager
        .handlerSpy(TestEvents.created.snsTopic)
        .waitForMessageWithId(publishedMessage.id)

      const consumerResult = await consumer.handlerSpy.waitForMessageWithId(publishedMessage.id)
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
          newData: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })

      await consumer.close()
    })

    it('message publishing is type-safe', async () => {
      await expect(
        publisherManager.publish(TestEvents.created.snsTopic, {
          payload: {
            // @ts-expect-error This should be causing a compilation error
            updatedData: 'edwe',
          },
          type: 'entity.created',
        }),
      ).rejects.toThrow(/invalid_type/)
    })

    it('publish to a non-existing topic will throw error', async () => {
      await expect(
        // @ts-expect-error Testing error scenario
        publisherManager.publish('non-existing-topic', {
          type: 'entity.created',
          payload: {
            newData: 'msg',
          },
        }),
      ).rejects.toThrow('No publisher for target non-existing-topic')
    })

    it('publish to an incorrect topic/message combination will throw error', async () => {
      await expect(
        publisherManager.publish(TestEvents.created.snsTopic, {
          // @ts-expect-error Testing error scenario
          type: 'dummy.type',
          payload: {
            newData: 'msg',
          },
        }),
      ).rejects.toThrow(
        'MessageDefinition for target "dummy" and type "dummy.type" not found in EventRegistry',
      )
    })
  })

  describe('handlerSpy', () => {
    it('returns correct handler spy', () => {
      const spy = publisherManager.handlerSpy(TestEvents.created.snsTopic)
      expect(spy).toBeDefined()
    })

    it('returns error when no publisher for topic', () => {
      // @ts-expect-error Testing incorrect scenario
      expect(() => publisherManager.handlerSpy('non-existing-topic')).toThrow(
        'No publisher for target non-existing-topic',
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
        messageSchemas: [TestEvents.created.publisherSchema],
      })

      // When
      const messageId = randomUUID()
      // @ts-ignore
      publisherManager.injectPublisher(topic, newPublisher)
      publisherManager.injectEventDefinition({
        ...enrichMessageSchemaWithBase('entity.created', z.object({}).catchall(z.any())),
        snsTopic: topic,
        schemaVersion: '2.0.0',
      })

      // @ts-expect-error Testing injected publisher
      await publisherManager.publish(topic, {
        id: messageId,
        type: 'entity.created',
        payload: {
          newData: 'msg',
        },
      })

      // Then
      const spyRes = await publisherManager
        // @ts-expect-error Testing injected publisher
        .handlerSpy(topic)
        .waitForMessageWithId(messageId, 'published')
      expect(spyRes.processingResult).toBe('published')
    })
  })
})
