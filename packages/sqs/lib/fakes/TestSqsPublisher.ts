import type { SQSClient } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'

import type { AbstractSqsConsumer } from '../sqs/AbstractSqsConsumer.ts'
import type { AbstractSqsPublisher } from '../sqs/AbstractSqsPublisher.ts'
import { getQueueUrl } from '../utils/sqsUtils.ts'

/**
 * TestSqsPublisher - A testing utility for publishing arbitrary messages to SQS queues without validation.
 *
 * This publisher bypasses all message validation, schema checking, deduplication, and payload offloading
 * to enable testing edge cases, invalid messages, and integration scenarios.
 *
 * **IMPORTANT: This is a testing utility only. Do not use in production code.**
 *
 * Features:
 * - Publish any JSON-serializable payload to any SQS queue
 * - No Zod schema validation
 * - No message deduplication
 * - No payload offloading
 * - Accept queueUrl string or consumer/publisher instance per publish call
 * - Automatically resolve queueUrl from consumer/publisher
 * - Support FIFO queue options (MessageGroupId, MessageDeduplicationId)
 *
 * @example
 * ```typescript
 * // Create a single publisher for all queues
 * const publisher = new TestSqsPublisher(sqsClient)
 *
 * // Publish to different queues
 * await publisher.publish({ any: 'data' }, { queueUrl: 'https://sqs...' })
 * await publisher.publish({ test: 'message' }, { queueName: 'my-queue' })
 * await publisher.publish({ test: 'message' }, { consumer: myConsumer })
 * await publisher.publish({ other: 'data' }, { publisher: myPublisher })
 *
 * // FIFO queue with options
 * await publisher.publish(
 *   { test: 'message' },
 *   {
 *     queueUrl: 'https://sqs.../my-queue.fifo',
 *     MessageGroupId: 'group1',
 *     MessageDeduplicationId: 'unique-id'
 *   }
 * )
 * ```
 */
export class TestSqsPublisher {
  private readonly sqsClient: SQSClient

  /**
   * Creates a new TestSqsPublisher instance.
   *
   * @param sqsClient - AWS SQS client instance
   */
  constructor(sqsClient: SQSClient) {
    this.sqsClient = sqsClient
  }

  /**
   * Publishes a message to an SQS queue without any validation.
   *
   * @param payload - Any JSON-serializable object to publish
   * @param options - Queue and message options
   * @param options.queueUrl - Direct queue URL (mutually exclusive with queueName/consumer/publisher)
   * @param options.queueName - Queue name to resolve to URL (mutually exclusive with queueUrl/consumer/publisher)
   * @param options.consumer - Consumer instance to extract queue URL from (mutually exclusive with queueUrl/queueName/publisher)
   * @param options.publisher - Publisher instance to extract queue URL from (mutually exclusive with queueUrl/queueName/consumer)
   * @param options.MessageGroupId - Required for FIFO queues (queue name ends with .fifo)
   * @param options.MessageDeduplicationId - Optional deduplication ID for FIFO queues
   *
   * @returns Promise that resolves when message is published
   * @throws {Error} If none of queueUrl/queueName/consumer/publisher is provided
   * @throws {Error} If consumer/publisher has not been initialized (no queueUrl available)
   * @throws {Error} If SQS publish fails
   *
   * @example
   * ```typescript
   * // Using with a queue URL
   * await publisher.publish({ test: 'data' }, { queueUrl: 'https://sqs...' })
   *
   * // Using with a queue name
   * await publisher.publish({ test: 'data' }, { queueName: 'my-queue' })
   *
   * // Using with a consumer
   * await publisher.publish({ test: 'data' }, { consumer: myConsumer })
   *
   * // Using with a publisher
   * await publisher.publish({ test: 'data' }, { publisher: myPublisher })
   *
   * // FIFO queue
   * await publisher.publish(
   *   { test: 'data' },
   *   {
   *     queueUrl: 'https://sqs.../queue.fifo',
   *     MessageGroupId: 'group1',
   *     MessageDeduplicationId: 'unique-id'
   *   }
   * )
   * ```
   */
  async publish(
    payload: unknown,
    options:
      | {
          queueUrl: string
          queueName?: never
          consumer?: never
          publisher?: never
          MessageGroupId?: string
          MessageDeduplicationId?: string
        }
      | {
          queueName: string
          queueUrl?: never
          consumer?: never
          publisher?: never
          MessageGroupId?: string
          MessageDeduplicationId?: string
        }
      | {
          consumer: AbstractSqsConsumer<
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
          queueUrl?: never
          queueName?: never
          publisher?: never
          MessageGroupId?: string
          MessageDeduplicationId?: string
        }
      | {
          // biome-ignore lint/suspicious/noExplicitAny: Allow any message type for testing
          publisher: AbstractSqsPublisher<any>
          queueUrl?: never
          queueName?: never
          consumer?: never
          MessageGroupId?: string
          MessageDeduplicationId?: string
        },
  ): Promise<void> {
    let queueUrl: string

    if (options.queueUrl) {
      queueUrl = options.queueUrl
    } else if (options.queueName) {
      // Resolve queue name to URL
      const result = await getQueueUrl(this.sqsClient, options.queueName)
      if (result.error) {
        throw result.error
      }
      queueUrl = result.result
    } else if (options.consumer) {
      // Extract queueUrl from consumer
      // @ts-expect-error - Accessing protected property for testing purposes
      const consumerQueueUrl = options.consumer.queueUrl as string | undefined
      if (!consumerQueueUrl) {
        throw new Error(
          'Consumer has not been initialized. Call consumer.init() before passing it to TestSqsPublisher.publish().',
        )
      }
      queueUrl = consumerQueueUrl
    } else if (options.publisher) {
      // Extract queueUrl from publisher
      // @ts-expect-error - Accessing protected property for testing purposes
      const publisherQueueUrl = options.publisher.queueUrl as string | undefined
      if (!publisherQueueUrl) {
        throw new Error(
          'Publisher has not been initialized. Call publisher.init() or publisher.publish() before passing it to TestSqsPublisher.publish().',
        )
      }
      queueUrl = publisherQueueUrl
    } else {
      throw new Error(
        'Either queueUrl, queueName, consumer, or publisher must be provided in options',
      )
    }

    const command = new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(payload),
      MessageGroupId: options.MessageGroupId,
      MessageDeduplicationId: options.MessageDeduplicationId,
    })

    await this.sqsClient.send(command)
  }
}
