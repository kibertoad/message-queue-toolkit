import type { Either } from '@lokalise/node-core'
import { MessageHandlerConfigBuilder, NO_MESSAGE_TYPE_FIELD } from '@message-queue-toolkit/core'
import { z } from 'zod/v4'
import {
  AbstractPubSubConsumer,
  type PubSubConsumerDependencies,
  type PubSubConsumerOptions,
} from './AbstractPubSubConsumer.ts'
import type { PubSubCreationConfig, PubSubQueueLocatorType } from './AbstractPubSubService.ts'

/**
 * Base schema for DLQ messages.
 * Uses passthrough() to accept any message structure while ensuring basic fields exist.
 * The 'id' field is required for handler spy functionality.
 */
export const DLQ_MESSAGE_SCHEMA = z
  .object({
    id: z.string(),
  })
  .passthrough()

export type DlqMessage = z.infer<typeof DLQ_MESSAGE_SCHEMA>

/**
 * Handler function type for processing DLQ messages.
 */
export type DlqMessageHandler<ExecutionContext> = (
  message: DlqMessage,
  context: ExecutionContext,
) => Promise<Either<'retryLater', 'success'>>

/**
 * Options for AbstractPubSubDlqConsumer.
 * Omits messageTypeField and handlers since DLQ consumers handle all message types uniformly.
 */
export type PubSubDlqConsumerOptions<
  ExecutionContext,
  CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
  QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
> = Omit<
  PubSubConsumerOptions<
    DlqMessage,
    ExecutionContext,
    undefined,
    CreationConfigType,
    QueueLocatorType
  >,
  'messageTypeField' | 'handlers'
> & {
  /**
   * Handler function to process DLQ messages.
   * Receives the raw message payload with passthrough fields.
   */
  handler: DlqMessageHandler<ExecutionContext>
}

/**
 * Abstract base class for Dead Letter Queue (DLQ) consumers.
 *
 * This class is specifically designed for consuming messages from a DLQ topic.
 * Unlike regular consumers that route messages by type, DLQ consumers accept
 * any message structure since DLQ messages can come from various failed processing scenarios.
 *
 * Key differences from AbstractPubSubConsumer:
 * - Does NOT use messageTypeField routing (accepts all message types)
 * - Uses a passthrough schema that accepts any message with an 'id' field
 * - Simplified handler configuration (single handler for all messages)
 *
 * @example
 * ```typescript
 * class MyDlqConsumer extends AbstractPubSubDlqConsumer<MyContext> {
 *   constructor(dependencies: PubSubConsumerDependencies) {
 *     super(
 *       dependencies,
 *       {
 *         creationConfig: {
 *           topic: { name: 'my-dlq-topic' },
 *           subscription: { name: 'my-dlq-subscription' },
 *         },
 *         handler: async (message, context) => {
 *           console.log('DLQ message received:', message)
 *           // Process or log the dead letter message
 *           return { result: 'success' }
 *         },
 *       },
 *       myExecutionContext,
 *     )
 *   }
 * }
 * ```
 */
export abstract class AbstractPubSubDlqConsumer<
  ExecutionContext = Record<string, never>,
  CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
  QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
> extends AbstractPubSubConsumer<
  DlqMessage,
  ExecutionContext,
  undefined,
  CreationConfigType,
  QueueLocatorType
> {
  constructor(
    dependencies: PubSubConsumerDependencies,
    options: PubSubDlqConsumerOptions<ExecutionContext, CreationConfigType, QueueLocatorType>,
    executionContext: ExecutionContext,
  ) {
    const { handler, ...restOptions } = options

    super(
      dependencies,
      {
        ...restOptions,
        // NO_MESSAGE_TYPE_FIELD ensures all messages use the default handler
        // for both schema storage and lookup, allowing a single handler for any message type
        messageTypeField: NO_MESSAGE_TYPE_FIELD,
        handlers: new MessageHandlerConfigBuilder<DlqMessage, ExecutionContext>()
          .addConfig(DLQ_MESSAGE_SCHEMA, (message, context) => handler(message, context))
          .build(),
      },
      executionContext,
    )
  }
}
