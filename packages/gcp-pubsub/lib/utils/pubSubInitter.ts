import type { PubSub, Subscription, Topic } from '@google-cloud/pubsub'
import type { DeletionConfig } from '@message-queue-toolkit/core'
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
      // TODO: Support topic options (messageRetentionDuration, messageStoragePolicy, etc.)
      // The topic.create() method doesn't accept these options directly
      // Need to investigate proper API for setting topic configuration
      const [createdTopic] = await topic.create()
      topic = createdTopic
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
      } else if (dlqTopic && deadLetterQueueConfig) {
        // Update existing subscription with deadLetterPolicy
        await subscription.setMetadata({
          deadLetterPolicy: {
            deadLetterTopic: dlqTopic.name,
            maxDeliveryAttempts: deadLetterQueueConfig.deadLetterPolicy.maxDeliveryAttempts,
          },
        })
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

export async function deletePubSub(
  pubSubClient: PubSub,
  deletionConfig: DeletionConfig,
  creationConfig?: PubSubCreationConfig,
): Promise<void> {
  if (!deletionConfig.deleteIfExists || !creationConfig) {
    return
  }

  // Delete subscription first (if it exists)
  if (creationConfig.subscription) {
    const subscription = pubSubClient.subscription(creationConfig.subscription.name)
    const [subscriptionExists] = await subscription.exists()
    if (subscriptionExists) {
      await subscription.delete()
    }
  }

  // Delete topic
  const topic = pubSubClient.topic(creationConfig.topic.name)
  const [topicExists] = await topic.exists()
  if (topicExists) {
    await topic.delete()
  }
}
