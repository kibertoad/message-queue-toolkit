import type { PubSub } from '@google-cloud/pubsub'

export async function deletePubSubTopic(pubSubClient: PubSub, topicName: string): Promise<void> {
  const topic = pubSubClient.topic(topicName)
  const [exists] = await topic.exists()
  if (exists) {
    await topic.delete()
  }
}

export async function deletePubSubSubscription(
  pubSubClient: PubSub,
  topicName: string,
  subscriptionName: string,
): Promise<void> {
  const topic = pubSubClient.topic(topicName)
  const [topicExists] = await topic.exists()
  if (topicExists) {
    const subscription = topic.subscription(subscriptionName)
    const [subExists] = await subscription.exists()
    if (subExists) {
      await subscription.delete()
    }
  }
}

export async function deletePubSubTopicAndSubscription(
  pubSubClient: PubSub,
  topicName: string,
  subscriptionName: string,
): Promise<void> {
  const topic = pubSubClient.topic(topicName)
  const [topicExists] = await topic.exists()
  if (topicExists) {
    const subscription = topic.subscription(subscriptionName)
    const [subExists] = await subscription.exists()
    if (subExists) {
      await subscription.delete()
    }
    await topic.delete()
  }
}
