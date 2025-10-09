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
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: topic/subscription initialization requires complex logic
export async function initPubSub(
  pubSubClient: PubSub,
  locatorConfig?: PubSubQueueLocatorType,
  creationConfig?: PubSubCreationConfig,
): Promise<PubSubInitResult> {
  if (!locatorConfig && !creationConfig) {
    throw new Error('Either locatorConfig or creationConfig must be provided')
  }

  let topic: Topic
  let topicName: string
  let subscription: Subscription | undefined
  let subscriptionName: string | undefined

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

    // Create subscription if config provided (for consumers)
    if (creationConfig.subscription) {
      subscriptionName = creationConfig.subscription.name
      subscription = topic.subscription(subscriptionName)

      const [subscriptionExists] = await subscription.exists()
      if (!subscriptionExists) {
        const [createdSubscription] = await topic.createSubscription(
          subscriptionName,
          creationConfig.subscription.options,
        )
        subscription = createdSubscription
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
