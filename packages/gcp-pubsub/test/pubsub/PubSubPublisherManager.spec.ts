import { randomUUID } from 'node:crypto'

import {
  CommonMetadataFiller,
  EventRegistry,
  enrichMessageSchemaWithBase,
} from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import z from 'zod/v4'
import { CommonPubSubPublisher } from '../../lib/pubsub/CommonPubSubPublisherFactory.ts'
import type { PubSubAwareEventDefinition } from '../../lib/pubsub/PubSubPublisherManager.ts'
import { PubSubPublisherManager } from '../../lib/pubsub/PubSubPublisherManager.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

const TestEvents = {
  created: {
    ...enrichMessageSchemaWithBase(
      'entity.created',
      z.object({
        newData: z.string(),
      }),
    ),
    schemaVersion: '1.0.1',
    pubSubTopic: 'test-topic-created',
  },

  updated: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        updatedData: z.string(),
      }),
    ),
    pubSubTopic: 'test-topic-updated',
  },
} as const satisfies Record<string, PubSubAwareEventDefinition>

type TestEventsType = (typeof TestEvents)[keyof typeof TestEvents][]
type TestEventPublishPayloadsType = z.output<TestEventsType[number]['publisherSchema']>

describe('PubSubPublisherManager', () => {
  let diContainer: AwilixContainer<Dependencies>
  let publisherManager: PubSubPublisherManager<
    CommonPubSubPublisher<TestEventPublishPayloadsType>,
    TestEventsType
  >

  beforeAll(async () => {
    diContainer = await registerDependencies()

    const eventRegistry = new EventRegistry(Object.values(TestEvents))

    publisherManager = new PubSubPublisherManager(
      {
        ...diContainer.cradle,
        eventRegistry,
      },
      {
        metadataField: 'metadata',
        metadataFiller: new CommonMetadataFiller({
          serviceId: 'test-service',
        }),
        newPublisherOptions: {
          handlerSpy: true,
          messageTypeResolver: { messageTypePath: 'type' },
          logMessages: false,
        },
      },
    )

    await publisherManager.initRegisteredPublishers()
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('publish', () => {
    it('publishes to a correct publisher', async () => {
      // When
      const publishedMessage = await publisherManager.publish(TestEvents.created.pubSubTopic, {
        payload: {
          newData: 'msg',
        },
        type: 'entity.created',
      })

      const publishedMessageResult = await publisherManager
        .handlerSpy(TestEvents.created.pubSubTopic)
        .waitForMessageWithId(publishedMessage.id)

      expect(publishedMessageResult.processingResult.status).toEqual('published')

      expect(publishedMessage).toMatchObject({
        id: expect.any(String),
        metadata: {
          correlationId: expect.any(String),
          originatedFrom: 'test-service',
          producedBy: 'test-service',
          schemaVersion: '1.0.1',
        },
        payload: {
          newData: 'msg',
        },
        timestamp: expect.any(String),
        type: 'entity.created',
      })
    })

    it('publishes to different topics', async () => {
      // Publish to created topic
      const createdMessage = await publisherManager.publish(TestEvents.created.pubSubTopic, {
        payload: {
          newData: 'created msg',
        },
        type: 'entity.created',
      })

      // Publish to updated topic
      const updatedMessage = await publisherManager.publish(TestEvents.updated.pubSubTopic, {
        payload: {
          updatedData: 'updated msg',
        },
        type: 'entity.updated',
      })

      const createdResult = await publisherManager
        .handlerSpy(TestEvents.created.pubSubTopic)
        .waitForMessageWithId(createdMessage.id)

      const updatedResult = await publisherManager
        .handlerSpy(TestEvents.updated.pubSubTopic)
        .waitForMessageWithId(updatedMessage.id)

      expect(createdResult.processingResult.status).toEqual('published')
      expect(updatedResult.processingResult.status).toEqual('published')
      expect(createdMessage.type).toBe('entity.created')
      expect(updatedMessage.type).toBe('entity.updated')
    })

    it('message publishing is type-safe', async () => {
      await expect(
        publisherManager.publish(TestEvents.created.pubSubTopic, {
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
        publisherManager.publish(TestEvents.created.pubSubTopic, {
          // @ts-expect-error Testing error scenario
          type: 'dummy.type',
          payload: {
            newData: 'msg',
          },
        }),
      ).rejects.toThrow(
        'MessageDefinition for target "test-topic-created" and type "dummy.type" not found in EventRegistry',
      )
    })
  })

  describe('handlerSpy', () => {
    it('returns correct handler spy', () => {
      const spy = publisherManager.handlerSpy(TestEvents.created.pubSubTopic)
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
      const topic = 'test-injected-topic'
      const injectedSchema = enrichMessageSchemaWithBase(
        'entity.created',
        z.object({}).catchall(z.any()),
      )
      const newPublisher = new CommonPubSubPublisher(diContainer.cradle, {
        creationConfig: {
          topic: {
            name: topic,
          },
        },
        handlerSpy: true,
        messageTypeResolver: { messageTypePath: 'type' },
        messageSchemas: [injectedSchema.consumerSchema],
      })

      await newPublisher.init()

      // When
      const messageId = randomUUID()
      // @ts-expect-error
      publisherManager.injectPublisher(topic, newPublisher)
      publisherManager.injectEventDefinition({
        ...injectedSchema,
        pubSubTopic: topic,
        schemaVersion: '2.0.0',
      })

      // Then
      const { timestamp } = publisherManager.resolveBaseFields()
      await publisherManager.publish(
        // @ts-expect-error
        topic,
        {
          id: messageId,
          type: 'entity.created',
          timestamp,
          payload: {},
        },
      )

      const result = await newPublisher.handlerSpy.waitForMessageWithId(messageId)
      expect(result.processingResult.status).toBe('published')

      await newPublisher.close()
    })
  })
})
