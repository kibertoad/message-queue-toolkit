import type { PubSub } from '@google-cloud/pubsub'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { deletePubSubTopic } from '../utils/cleanupPubSub.ts'
import { registerDependencies } from '../utils/testContext.ts'
import { PubSubPermissionPublisher } from './PubSubPermissionPublisher.ts'

describe('PubSubPermissionPublisher', () => {
  let diContainer: Awaited<ReturnType<typeof registerDependencies>>
  let publisher: PubSubPermissionPublisher
  let pubSubClient: PubSub

  beforeAll(async () => {
    diContainer = await registerDependencies()
    publisher = diContainer.cradle.permissionPublisher
    pubSubClient = diContainer.cradle.pubSubClient

    // Clean up - delete topic if exists before tests start
    await deletePubSubTopic(pubSubClient, PubSubPermissionPublisher.TOPIC_NAME)
  })

  afterAll(async () => {
    await diContainer.dispose()
  })

  describe('init', () => {
    it('creates a new topic', async () => {
      const newPublisher = diContainer.cradle.permissionPublisher

      await newPublisher.init()

      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)

      await newPublisher.close()
    })

    it('does not throw an error when initiated twice', async () => {
      const newPublisher = diContainer.cradle.permissionPublisher

      await newPublisher.init()
      await newPublisher.init()

      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)

      await newPublisher.close()
    })
  })

  describe('publish', () => {
    it('publishes a message to topic', async () => {
      const message = {
        id: '1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId1'],
      }

      await publisher.publish(message)

      // Verify message was published (topic should exist and have been used)
      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)
    })

    it('publishes multiple messages', async () => {
      const message1 = {
        id: '1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId1'],
      }

      const message2 = {
        id: '2',
        messageType: 'remove' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId2'],
      }

      await publisher.publish(message1)
      await publisher.publish(message2)

      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)
    })

    it('publishes message with ordering key', async () => {
      const message = {
        id: '1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId1'],
      }

      await publisher.publish(message, { orderingKey: 'user-123' })

      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)
    })

    it('publishes message with custom attributes', async () => {
      const message = {
        id: '1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId1'],
      }

      await publisher.publish(message, {
        attributes: {
          customKey: 'customValue',
        },
      })

      const [exists] = await pubSubClient.topic(PubSubPermissionPublisher.TOPIC_NAME).exists()
      expect(exists).toBe(true)
    })
  })

  describe('handler spy', () => {
    it('records published messages', async () => {
      const newPublisher = diContainer.cradle.permissionPublisher

      const message = {
        id: 'spy-test-1',
        messageType: 'add' as const,
        timestamp: new Date().toISOString(),
        userIds: ['userId1'],
      }

      await newPublisher.publish(message)

      const spy = newPublisher.handlerSpy
      const spyResult = await spy.waitForMessageWithId('spy-test-1', 'published')

      expect(spyResult).toBeDefined()
      expect(spyResult.message.id).toBe('spy-test-1')
      expect(spyResult.processingResult.status).toBe('published')

      await newPublisher.close()
    })
  })
})
