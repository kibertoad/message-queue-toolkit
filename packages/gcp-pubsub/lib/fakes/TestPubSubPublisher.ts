import type { PubSub } from '@google-cloud/pubsub'

import type { AbstractPubSubConsumer } from '../pubsub/AbstractPubSubConsumer.ts'
import type { AbstractPubSubPublisher } from '../pubsub/AbstractPubSubPublisher.ts'

/**
 * Options for publishing messages with TestPubSubPublisher.
 * Supports multiple mutually exclusive ways to specify the target topic.
 */
export type TestPubSubPublishOptions =
  | {
      topicName: string
      consumer?: never
      publisher?: never
      orderingKey?: string
      attributes?: Record<string, string>
    }
  | {
      consumer: AbstractPubSubConsumer<
        // biome-ignore lint/suspicious/noExplicitAny: Allow any message type for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any context for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any prehandler output for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any creation config for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any locator type for testing
        any
      >
      topicName?: never
      publisher?: never
      orderingKey?: string
      attributes?: Record<string, string>
    }
  | {
      // biome-ignore lint/suspicious/noExplicitAny: Allow any message type for testing
      publisher: AbstractPubSubPublisher<any>
      topicName?: never
      consumer?: never
      orderingKey?: string
      attributes?: Record<string, string>
    }

/**
 * TestPubSubPublisher - A testing utility for publishing arbitrary messages to GCP Pub/Sub topics without validation.
 *
 * This publisher bypasses all message validation, schema checking, deduplication, and payload offloading
 * to enable testing edge cases, invalid messages, and integration scenarios.
 *
 * **IMPORTANT: This is a testing utility only. Do not use in production code.**
 *
 * Features:
 * - Publish any JSON-serializable payload to any Pub/Sub topic
 * - No Zod schema validation
 * - No message deduplication
 * - No payload offloading
 * - Accept topicName string or consumer/publisher instance per publish call
 * - Automatically resolve topicName from consumer/publisher
 * - Support Pub/Sub ordering keys and attributes
 *
 * @example
 * ```typescript
 * // Create a single publisher for all topics
 * const publisher = new TestPubSubPublisher(pubSubClient)
 *
 * // Publish to different topics
 * await publisher.publish({ any: 'data' }, { topicName: 'my-topic' })
 * await publisher.publish({ test: 'message' }, { consumer: myConsumer })
 * await publisher.publish({ other: 'data' }, { publisher: myPublisher })
 *
 * // With ordering key and attributes
 * await publisher.publish(
 *   { test: 'message' },
 *   {
 *     topicName: 'my-topic',
 *     orderingKey: 'order-1',
 *     attributes: { key: 'value' }
 *   }
 * )
 * ```
 */
export class TestPubSubPublisher {
  private readonly pubSubClient: PubSub

  /**
   * Creates a new TestPubSubPublisher instance.
   *
   * @param pubSubClient - GCP Pub/Sub client instance
   */
  constructor(pubSubClient: PubSub) {
    this.pubSubClient = pubSubClient
  }

  /**
   * Publishes a message to a Pub/Sub topic without any validation.
   *
   * @param payload - Any JSON-serializable object to publish
   * @param options - Topic and message options
   * @param options.topicName - Topic name (mutually exclusive with consumer/publisher)
   * @param options.consumer - Consumer instance to extract topic name from (mutually exclusive with topicName/publisher)
   * @param options.publisher - Publisher instance to extract topic name from (mutually exclusive with topicName/consumer)
   * @param options.orderingKey - Optional ordering key for ordered delivery
   * @param options.attributes - Optional message attributes
   *
   * @returns Promise that resolves when message is published
   * @throws {Error} If none of topicName/consumer/publisher is provided
   * @throws {Error} If consumer/publisher has not been initialized (no topicName available)
   * @throws {Error} If Pub/Sub publish fails
   *
   * @example
   * ```typescript
   * // Using with a topic name
   * await publisher.publish({ test: 'data' }, { topicName: 'my-topic' })
   *
   * // Using with a consumer
   * await publisher.publish({ test: 'data' }, { consumer: myConsumer })
   *
   * // Using with a publisher
   * await publisher.publish({ test: 'data' }, { publisher: myPublisher })
   *
   * // With ordering key
   * await publisher.publish(
   *   { test: 'data' },
   *   {
   *     topicName: 'my-topic',
   *     orderingKey: 'order-1'
   *   }
   * )
   * ```
   */
  async publish(payload: unknown, options: TestPubSubPublishOptions): Promise<void> {
    let topicName: string

    if (options.topicName) {
      topicName = options.topicName
    } else if (options.consumer) {
      // Extract topicName from consumer
      // @ts-expect-error - Accessing protected property for testing purposes
      const consumerTopicName = options.consumer.topicName as string | undefined
      if (!consumerTopicName) {
        throw new Error(
          'Consumer has not been initialized. Call consumer.start() before passing it to TestPubSubPublisher.publish().',
        )
      }
      topicName = consumerTopicName
    } else if (options.publisher) {
      // Extract topicName from publisher
      // @ts-expect-error - Accessing protected property for testing purposes
      const publisherTopicName = options.publisher.topicName as string | undefined
      if (!publisherTopicName) {
        throw new Error(
          'Publisher has not been initialized. Call publisher.init() or publisher.publish() before passing it to TestPubSubPublisher.publish().',
        )
      }
      topicName = publisherTopicName
    } else {
      throw new Error('Either topicName, consumer, or publisher must be provided in options')
    }

    const topic = this.pubSubClient.topic(topicName)
    const messageData = Buffer.from(JSON.stringify(payload))

    await topic.publishMessage({
      data: messageData,
      orderingKey: options.orderingKey,
      attributes: options.attributes,
    })
  }
}
