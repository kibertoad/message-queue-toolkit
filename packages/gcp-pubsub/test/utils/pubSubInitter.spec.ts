import type { PubSub } from '@google-cloud/pubsub'
import type { DeletionConfig } from '@message-queue-toolkit/core'
import { reloadConfig } from '@message-queue-toolkit/core'
import type { AwilixContainer } from 'awilix'
import { asValue } from 'awilix'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest'

import { deletePubSub, initPubSub } from '../../lib/utils/pubSubInitter.ts'
import { deletePubSubTopicAndSubscription } from '../utils/cleanupPubSub.ts'
import type { Dependencies } from '../utils/testContext.ts'
import { registerDependencies } from '../utils/testContext.ts'

describe('pubSubInitter', () => {
  let diContainer: AwilixContainer<Dependencies>
  let pubSubClient: PubSub

  beforeAll(async () => {
    diContainer = await registerDependencies({
      permissionPublisher: asValue(() => undefined),
      permissionConsumer: asValue(() => undefined),
    })
    pubSubClient = diContainer.cradle.pubSubClient
  })

  afterAll(async () => {
    const { awilixManager } = diContainer.cradle
    await awilixManager.executeDispose()
    await diContainer.dispose()
  })

  describe('updateAttributesIfExists', () => {
    const topicName = 'test-update-attributes-topic'
    const subscriptionName = 'test-update-attributes-subscription'

    afterEach(async () => {
      await deletePubSubTopicAndSubscription(pubSubClient, topicName, subscriptionName)
    })

    it('creates topic with options on first init', async () => {
      const result = await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
          options: {
            messageRetentionDuration: {
              seconds: 86400, // 1 day
            },
          },
        },
        updateAttributesIfExists: false,
      })

      expect(result.topicName).toBe(topicName)
      expect(result.topic).toBeDefined()

      // Verify topic was created with retention
      const [metadata] = await result.topic.getMetadata()
      expect(metadata.messageRetentionDuration).toBeDefined()
    }, 10000)

    it('updates existing topic attributes when updateAttributesIfExists is true', async () => {
      // Create topic first
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
          options: {
            messageRetentionDuration: {
              seconds: 86400, // 1 day
            },
          },
        },
      })

      // Update with different retention
      const result = await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
          options: {
            messageRetentionDuration: {
              seconds: 172800, // 2 days
            },
          },
        },
        updateAttributesIfExists: true,
      })

      expect(result.topicName).toBe(topicName)

      // Verify topic was updated
      const [metadata] = await result.topic.getMetadata()
      expect(metadata.messageRetentionDuration?.seconds).toBe('172800')
    })

    it('does not update topic attributes when updateAttributesIfExists is false', async () => {
      // Create topic first
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
          options: {
            messageRetentionDuration: {
              seconds: 86400, // 1 day
            },
          },
        },
      })

      const [originalMetadata] = await pubSubClient.topic(topicName).getMetadata()

      // Try to init again with different options but updateAttributesIfExists false
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
          options: {
            messageRetentionDuration: {
              seconds: 172800, // 2 days
            },
          },
        },
        updateAttributesIfExists: false,
      })

      // Verify topic was NOT updated
      const [currentMetadata] = await pubSubClient.topic(topicName).getMetadata()
      expect(currentMetadata.messageRetentionDuration).toEqual(
        originalMetadata.messageRetentionDuration,
      )
    })

    it('updates existing subscription attributes when updateAttributesIfExists is true', async () => {
      // Create topic and subscription first
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
          options: {
            ackDeadlineSeconds: 10,
          },
        },
      })

      // Update with different ackDeadlineSeconds
      const result = await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
          options: {
            ackDeadlineSeconds: 30,
          },
        },
        updateAttributesIfExists: true,
      })

      expect(result.subscription).toBeDefined()

      // Verify subscription was updated
      const [metadata] = await result.subscription!.getMetadata()
      expect(metadata.ackDeadlineSeconds).toBe(30)
    })

    it('does not update subscription attributes when updateAttributesIfExists is false', async () => {
      // Create topic and subscription first
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
          options: {
            ackDeadlineSeconds: 10,
          },
        },
      })

      // Try to init again with different options but updateAttributesIfExists false
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
          options: {
            ackDeadlineSeconds: 30,
          },
        },
        updateAttributesIfExists: false,
      })

      // Verify subscription was NOT updated
      const subscription = pubSubClient.subscription(subscriptionName)
      const [metadata] = await subscription.getMetadata()
      expect(metadata.ackDeadlineSeconds).toBe(10)
    })
  })

  describe('deletion behavior', () => {
    const topicName = 'test-deletion-topic'
    const subscriptionName = 'test-deletion-subscription'
    const originalNodeEnv = process.env.NODE_ENV

    beforeEach(async () => {
      // Create a test topic and subscription
      await initPubSub(pubSubClient, undefined, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
        },
      })
    })

    afterEach(async () => {
      // Restore original NODE_ENV
      if (originalNodeEnv !== undefined) {
        process.env.NODE_ENV = originalNodeEnv
      } else {
        delete process.env.NODE_ENV
      }

      // Clean up resources if they still exist
      try {
        await deletePubSubTopicAndSubscription(pubSubClient, topicName, subscriptionName)
      } catch {
        // Ignore errors - resources might already be deleted
      }
    })

    it('throws error when deleting in production without forceDeleteInProduction flag', async () => {
      process.env.NODE_ENV = 'production'
      reloadConfig() // Reload config to pick up env change

      const deletionConfig: DeletionConfig = {
        deleteIfExists: true,
        forceDeleteInProduction: false,
      }

      await expect(
        deletePubSub(pubSubClient, deletionConfig, {
          topic: {
            name: topicName,
          },
          subscription: {
            name: subscriptionName,
          },
        }),
      ).rejects.toThrow(/autodeletion in production/)
    })

    it('deletes resources with waitForConfirmation true (default)', async () => {
      process.env.NODE_ENV = 'development'
      reloadConfig() // Reload config to pick up env change

      const deletionConfig: DeletionConfig = {
        deleteIfExists: true,
        waitForConfirmation: true, // Explicitly set to true
      }

      await deletePubSub(pubSubClient, deletionConfig, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
        },
      })

      // Verify both resources are deleted
      const topic = pubSubClient.topic(topicName)
      const subscription = pubSubClient.subscription(subscriptionName)

      const [topicExists] = await topic.exists()
      const [subscriptionExists] = await subscription.exists()

      expect(topicExists).toBe(false)
      expect(subscriptionExists).toBe(false)
    })

    it('deletes resources with waitForConfirmation false', async () => {
      process.env.NODE_ENV = 'development'
      reloadConfig() // Reload config to pick up env change

      const deletionConfig: DeletionConfig = {
        deleteIfExists: true,
        waitForConfirmation: false, // Don't wait for confirmation
      }

      await deletePubSub(pubSubClient, deletionConfig, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
        },
      })

      // Give it a moment since we're not waiting for confirmation
      await new Promise((resolve) => setTimeout(resolve, 100))

      // Verify resources are eventually deleted (may take a bit longer)
      const topic = pubSubClient.topic(topicName)
      const subscription = pubSubClient.subscription(subscriptionName)

      const [topicExists] = await topic.exists()
      const [subscriptionExists] = await subscription.exists()

      expect(topicExists).toBe(false)
      expect(subscriptionExists).toBe(false)
    })

    it('allows deletion in production when forceDeleteInProduction is true', async () => {
      process.env.NODE_ENV = 'production'
      reloadConfig() // Reload config to pick up env change

      const deletionConfig: DeletionConfig = {
        deleteIfExists: true,
        forceDeleteInProduction: true,
      }

      await deletePubSub(pubSubClient, deletionConfig, {
        topic: {
          name: topicName,
        },
        subscription: {
          name: subscriptionName,
        },
      })

      // Verify resources were deleted
      const topic = pubSubClient.topic(topicName)
      const subscription = pubSubClient.subscription(subscriptionName)

      const [topicExists] = await topic.exists()
      const [subscriptionExists] = await subscription.exists()

      expect(topicExists).toBe(false)
      expect(subscriptionExists).toBe(false)
    })
  })

  describe('config validation', () => {
    it('throws error when both locatorConfig and creationConfig are provided', async () => {
      await expect(
        initPubSub(pubSubClient, { topicName: 'some-topic' }, { topic: { name: 'some-topic' } }),
      ).rejects.toThrow(/Cannot provide both/)
    })

    it('throws error when neither locatorConfig nor creationConfig are provided', async () => {
      await expect(initPubSub(pubSubClient, undefined, undefined)).rejects.toThrow(
        /Either locatorConfig or creationConfig must be provided/,
      )
    })
  })
})
