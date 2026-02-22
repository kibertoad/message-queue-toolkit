import type { SNSClient } from '@aws-sdk/client-sns'
import { PublishCommand } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'

import { buildTopicArn } from '../../utils/stsUtils.ts'
import type { AbstractSnsPublisher } from '../AbstractSnsPublisher.ts'
import type { AbstractSnsSqsConsumer } from '../AbstractSnsSqsConsumer.ts'

/**
 * Options for publishing messages with TestSnsPublisher.
 * Supports multiple mutually exclusive ways to specify the target topic.
 */
export type TestSnsPublishOptions =
  | {
      topicArn: string
      topicName?: never
      consumer?: never
      publisher?: never
      MessageGroupId?: string
      MessageDeduplicationId?: string
    }
  | {
      topicName: string
      topicArn?: never
      consumer?: never
      publisher?: never
      MessageGroupId?: string
      MessageDeduplicationId?: string
    }
  | {
      consumer: AbstractSnsSqsConsumer<
        // biome-ignore lint/suspicious/noExplicitAny: Allow any message type for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any context for testing
        any,
        // biome-ignore lint/suspicious/noExplicitAny: Allow any prehandler output for testing
        any
      >
      topicArn?: never
      topicName?: never
      publisher?: never
      MessageGroupId?: string
      MessageDeduplicationId?: string
    }
  | {
      // biome-ignore lint/suspicious/noExplicitAny: Allow any message type for testing
      publisher: AbstractSnsPublisher<any>
      topicArn?: never
      topicName?: never
      consumer?: never
      MessageGroupId?: string
      MessageDeduplicationId?: string
    }

/**
 * TestSnsPublisher - A testing utility for publishing arbitrary messages to SNS topics without validation.
 *
 * This publisher bypasses all message validation, schema checking, deduplication, and payload offloading
 * to enable testing edge cases, invalid messages, and integration scenarios.
 *
 * **IMPORTANT: This is a testing utility only. Do not use in production code.**
 *
 * Features:
 * - Publish any JSON-serializable payload to any SNS topic
 * - No Zod schema validation
 * - No message deduplication
 * - No payload offloading
 * - Accept topicArn string or consumer/publisher instance per publish call
 * - Automatically resolve topicArn from consumer/publisher
 * - Support FIFO topic options (MessageGroupId, MessageDeduplicationId)
 *
 * @example
 * ```typescript
 * // Create a single publisher for all topics
 * const publisher = new TestSnsPublisher(snsClient, stsClient)
 *
 * // Publish to different topics
 * await publisher.publish({ any: 'data' }, { topicArn: 'arn:aws:sns:...' })
 * await publisher.publish({ test: 'message' }, { topicName: 'my-topic' })
 * await publisher.publish({ test: 'message' }, { consumer: myConsumer })
 * await publisher.publish({ other: 'data' }, { publisher: myPublisher })
 *
 * // FIFO topic with options
 * await publisher.publish(
 *   { test: 'message' },
 *   {
 *     topicArn: 'arn:aws:sns:.../my-topic.fifo',
 *     MessageGroupId: 'group1',
 *     MessageDeduplicationId: 'unique-id'
 *   }
 * )
 * ```
 */
export class TestSnsPublisher {
  private readonly snsClient: SNSClient
  private readonly stsClient: STSClient

  /**
   * Creates a new TestSnsPublisher instance.
   *
   * @param snsClient - AWS SNS client instance
   * @param stsClient - AWS STS client instance (needed for topic name to ARN resolution)
   */
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    this.snsClient = snsClient
    this.stsClient = stsClient
  }

  /**
   * Publishes a message to an SNS topic without any validation.
   *
   * @param payload - Any JSON-serializable object to publish
   * @param options - Topic and message options
   * @param options.topicArn - Direct topic ARN (mutually exclusive with topicName/consumer/publisher)
   * @param options.topicName - Topic name to resolve to ARN (mutually exclusive with topicArn/consumer/publisher)
   * @param options.consumer - Consumer instance to extract topic ARN from (mutually exclusive with topicArn/topicName/publisher)
   * @param options.publisher - Publisher instance to extract topic ARN from (mutually exclusive with topicArn/topicName/consumer)
   * @param options.MessageGroupId - Required for FIFO topics (topic name ends with .fifo)
   * @param options.MessageDeduplicationId - Optional deduplication ID for FIFO topics
   *
   * @returns Promise that resolves when message is published
   * @throws {Error} If none of topicArn/topicName/consumer/publisher is provided
   * @throws {Error} If consumer/publisher has not been initialized (no topicArn available)
   * @throws {Error} If SNS publish fails
   */
  async publish(payload: unknown, options: TestSnsPublishOptions): Promise<void> {
    let topicArn: string

    if (options.topicArn) {
      topicArn = options.topicArn
    } else if (options.topicName) {
      topicArn = await buildTopicArn(this.stsClient, options.topicName)
    } else if (options.consumer) {
      // @ts-expect-error - Accessing protected property for testing purposes
      const consumerTopicArn = options.consumer.topicArn as string | undefined
      if (!consumerTopicArn) {
        throw new Error(
          'Consumer has not been initialized. Call consumer.init() before passing it to TestSnsPublisher.publish().',
        )
      }
      topicArn = consumerTopicArn
    } else if (options.publisher) {
      // @ts-expect-error - Accessing protected property for testing purposes
      const publisherTopicArn = options.publisher.topicArn as string | undefined
      if (!publisherTopicArn) {
        throw new Error(
          'Publisher has not been initialized. Call publisher.init() or publisher.publish() before passing it to TestSnsPublisher.publish().',
        )
      }
      topicArn = publisherTopicArn
    } else {
      throw new Error(
        'Either topicArn, topicName, consumer, or publisher must be provided in options',
      )
    }

    const command = new PublishCommand({
      TopicArn: topicArn,
      Message: JSON.stringify(payload),
      MessageGroupId: options.MessageGroupId,
      MessageDeduplicationId: options.MessageDeduplicationId,
    })

    await this.snsClient.send(command)
  }
}
