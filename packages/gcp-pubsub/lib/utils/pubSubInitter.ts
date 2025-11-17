import type { PubSub, Subscription, Topic } from '@google-cloud/pubsub'
import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction, waitAndRetry } from '@message-queue-toolkit/core'
import type {
  PubSubCreationConfig,
  PubSubQueueLocatorType,
} from '../pubsub/AbstractPubSubService.ts'

export type PubSubInitResult = {
  topicName: string
  topic: Topic
  subscriptionName?: string
  subscription?: Subscription
  dlqTopicName?: string
  dlqTopic?: Topic
}

export type PubSubDeadLetterQueueConfig = {
  deadLetterPolicy: {
    maxDeliveryAttempts: number
  }
  creationConfig?: {
    topic: {
      name: string
    }
  }
  locatorConfig?: {
    topicName: string
  }
}

/**
 * Initializes Pub/Sub resources (topics and subscriptions).
 *
 * Config precedence:
 * - If both locatorConfig and creationConfig are provided, an error is thrown
 * - locatorConfig: Locates existing resources and fails if they don't exist (production-safe)
 * - creationConfig: Creates resources if they don't exist, or uses existing ones (dev-friendly)
 *
 * The updateAttributesIfExists flag (creationConfig only):
 * - When true and resources already exist, their attributes/metadata will be updated via setMetadata()
 * - When false (default), existing resources are used as-is without updates
 * - Applies to both topics and subscriptions
 * - Useful for updating resource configurations without manual intervention
 * - Example use cases: updating retention policies, ack deadlines, or dead letter policies
 *
 * Dead Letter Queue configuration:
 * - Optional deadLetterQueueConfig parameter enables DLQ support for subscriptions
 * - Can use either locatorConfig (locate existing DLQ topic) or creationConfig (create if missing)
 * - Automatically configures the subscription's deadLetterPolicy with the DLQ topic ARN
 * - maxDeliveryAttempts determines how many times a message is retried before moving to DLQ
 */
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: topic/subscription initialization requires complex logic
export async function initPubSub(
  pubSubClient: PubSub,
  locatorConfig?: PubSubQueueLocatorType,
  creationConfig?: PubSubCreationConfig,
  deadLetterQueueConfig?: PubSubDeadLetterQueueConfig,
): Promise<PubSubInitResult> {
  if (!locatorConfig && !creationConfig) {
    throw new Error('Either locatorConfig or creationConfig must be provided')
  }

  if (locatorConfig && creationConfig) {
    throw new Error(
      'Cannot provide both locatorConfig and creationConfig. Use locatorConfig to locate existing resources or creationConfig to create resources.',
    )
  }

  let topic: Topic
  let topicName: string
  let subscription: Subscription | undefined
  let subscriptionName: string | undefined
  let dlqTopicName: string | undefined
  let dlqTopic: Topic | undefined

  if (locatorConfig) {
    // Locate existing resources
    topicName = locatorConfig.topicName
    topic = pubSubClient.topic(topicName)

    const [topicExists] = await topic.exists()
    if (!topicExists) {
      throw new Error(`Topic ${topicName} does not exist`)
    }

    if (locatorConfig.subscriptionName) {
      subscriptionName = locatorConfig.subscriptionName
      subscription = pubSubClient.subscription(subscriptionName)

      const [subscriptionExists] = await subscription.exists()
      if (!subscriptionExists) {
        throw new Error(`Subscription ${subscriptionName} does not exist`)
      }
    }
  } else if (creationConfig) {
    // Create resources if they don't exist
    topicName = creationConfig.topic.name
    topic = pubSubClient.topic(topicName)

    const [topicExists] = await topic.exists()
    if (!topicExists) {
      // Create topic first
      const [createdTopic] = await topic.create()
      topic = createdTopic

      // Set topic options if provided
      if (creationConfig.topic.options) {
        await topic.setMetadata(creationConfig.topic.options)
      }
    } else if (creationConfig.updateAttributesIfExists && creationConfig.topic.options) {
      // Update existing topic attributes if requested
      await topic.setMetadata(creationConfig.topic.options)
    }

    // Handle DLQ configuration if provided (before subscription creation)
    if (deadLetterQueueConfig) {
      // Resolve DLQ topic name from config
      if (deadLetterQueueConfig.locatorConfig) {
        dlqTopicName = deadLetterQueueConfig.locatorConfig.topicName
        dlqTopic = pubSubClient.topic(dlqTopicName)

        const [dlqTopicExists] = await dlqTopic.exists()
        if (!dlqTopicExists) {
          throw new Error(`Dead letter topic ${dlqTopicName} does not exist`)
        }
      } else if (deadLetterQueueConfig.creationConfig) {
        dlqTopicName = deadLetterQueueConfig.creationConfig.topic.name
        dlqTopic = pubSubClient.topic(dlqTopicName)

        const [dlqTopicExists] = await dlqTopic.exists()
        if (!dlqTopicExists) {
          const [createdDlqTopic] = await dlqTopic.create()
          dlqTopic = createdDlqTopic
        }
      } else {
        throw new Error('Either locatorConfig or creationConfig must be provided for DLQ')
      }
    }

    // Create subscription if config provided (for consumers)
    if (creationConfig.subscription) {
      subscriptionName = creationConfig.subscription.name
      subscription = topic.subscription(subscriptionName)

      const [subscriptionExists] = await subscription.exists()
      if (!subscriptionExists) {
        // Merge deadLetterPolicy with subscription options if DLQ is configured
        const subscriptionOptions = { ...creationConfig.subscription.options }
        if (dlqTopic && deadLetterQueueConfig) {
          subscriptionOptions.deadLetterPolicy = {
            deadLetterTopic: dlqTopic.name,
            maxDeliveryAttempts: deadLetterQueueConfig.deadLetterPolicy.maxDeliveryAttempts,
          }
        }

        const [createdSubscription] = await topic.createSubscription(
          subscriptionName,
          subscriptionOptions,
        )
        subscription = createdSubscription
      } else if (creationConfig.updateAttributesIfExists) {
        // Update existing subscription attributes if requested
        const updateOptions = { ...creationConfig.subscription.options }
        if (dlqTopic && deadLetterQueueConfig) {
          updateOptions.deadLetterPolicy = {
            deadLetterTopic: dlqTopic.name,
            maxDeliveryAttempts: deadLetterQueueConfig.deadLetterPolicy.maxDeliveryAttempts,
          }
        }

        if (Object.keys(updateOptions).length > 0) {
          await subscription.setMetadata(updateOptions)
        }
      }
    }
  } else {
    throw new Error('Unreachable code')
  }

  return {
    topicName,
    topic,
    subscriptionName,
    subscription,
    dlqTopicName,
    dlqTopic,
  }
}

/**
 * Deletes Pub/Sub resources (topics and subscriptions).
 *
 * Deletion behavior:
 * - Only deletes if deletionConfig.deleteIfExists is true and creationConfig is provided
 * - Checks forceDeleteInProduction flag to prevent accidental deletion in production environments
 * - Deletes subscription first (if exists), then topic
 * - If waitForConfirmation is true (default), polls to confirm resources are actually deleted
 *   using the core waitAndRetry utility (similar to SQS implementation)
 */
export async function deletePubSub(
  pubSubClient: PubSub,
  deletionConfig: DeletionConfig,
  creationConfig?: PubSubCreationConfig,
): Promise<void> {
  if (!deletionConfig.deleteIfExists || !creationConfig) {
    return
  }

  if (isProduction() && !deletionConfig.forceDeleteInProduction) {
    throw new Error(
      'You are running autodeletion in production. This can and probably will cause a loss of data. If you are absolutely sure you want to do this, please set deletionConfig.forceDeleteInProduction to true',
    )
  }

  const shouldWaitForConfirmation = deletionConfig.waitForConfirmation !== false

  // Delete subscription first (if it exists)
  if (creationConfig.subscription) {
    const subscriptionName = creationConfig.subscription.name
    const subscription = pubSubClient.subscription(subscriptionName)
    const [subscriptionExists] = await subscription.exists()
    if (subscriptionExists) {
      await subscription.delete()

      if (shouldWaitForConfirmation) {
        // Poll to confirm subscription is actually deleted
        await waitAndRetry(
          async () => {
            try {
              const [exists] = await subscription.exists()
              return !exists
            } catch {
              // If exists() throws an error, the resource is deleted
              return true
            }
          },
          100, // 100ms sleep between checks (vs default 20ms, since we're making API calls)
          15, // max 15 retry attempts (matches SQS default)
        )
      }
    }
  }

  // Delete topic
  const topicName = creationConfig.topic.name
  const topic = pubSubClient.topic(topicName)
  const [topicExists] = await topic.exists()
  if (topicExists) {
    await topic.delete()

    if (shouldWaitForConfirmation) {
      // Poll to confirm topic is actually deleted
      await waitAndRetry(
        async () => {
          try {
            const [exists] = await topic.exists()
            return !exists
          } catch {
            // If exists() throws an error, the resource is deleted
            return true
          }
        },
        100, // 100ms sleep between checks
        15, // max 15 retry attempts
      )
    }
  }
}
