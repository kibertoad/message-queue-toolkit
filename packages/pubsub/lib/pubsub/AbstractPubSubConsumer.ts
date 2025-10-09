import type { Either, ErrorResolver } from '@lokalise/node-core'
import {
  type BarrierResult,
  DeduplicationRequesterEnum,
  HandlerContainer,
  type MessageSchemaContainer,
  noopReleasableLock,
  type PreHandlingOutputs,
  type Prehandler,
  parseMessage,
  type QueueConsumer,
  type QueueConsumerDependencies,
  type QueueConsumerOptions,
  type TransactionObservabilityManager,
} from '@message-queue-toolkit/core'

import type { PubSubMessage } from '../types/MessageTypes.ts'
import { hasOffloadedPayload } from '../utils/messageUtils.ts'
import { deserializePubSubMessage } from '../utils/pubSubMessageDeserializer.ts'
import type {
  PubSubCreationConfig,
  PubSubDependencies,
  PubSubQueueLocatorType,
} from './AbstractPubSubService.ts'
import { AbstractPubSubService } from './AbstractPubSubService.ts'

const _ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}
const DEFAULT_MAX_RETRY_DURATION = 4 * 24 * 60 * 60 // 4 days in seconds

type PubSubDeadLetterQueueOptions = {
  deadLetterPolicy: {
    deadLetterTopic: string
    maxDeliveryAttempts: number
  }
}

export type PubSubConsumerDependencies = PubSubDependencies & QueueConsumerDependencies

export type PubSubConsumerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
  QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
> = QueueConsumerOptions<
  CreationConfigType,
  QueueLocatorType,
  PubSubDeadLetterQueueOptions,
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  PubSubCreationConfig,
  PubSubQueueLocatorType
> & {
  consumerOverrides?: {
    flowControl?: {
      maxMessages?: number
      maxBytes?: number
    }
    batching?: {
      maxMessages?: number
      maxMilliseconds?: number
    }
  }
}

export abstract class AbstractPubSubConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
    QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
    ConsumerOptionsType extends PubSubConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > = PubSubConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    >,
  >
  extends AbstractPubSubService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    PubSubConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly consumerOverrides: Partial<ConsumerOptionsType['consumerOverrides']>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >
  // Reserved for future DLQ implementation
  // biome-ignore lint/correctness/noUnusedPrivateClassMembers: Reserved for future dead letter queue implementation
  private readonly deadLetterQueueOptions?: PubSubDeadLetterQueueOptions
  private readonly isDeduplicationEnabled: boolean
  private maxRetryDuration: number
  private isConsuming = false

  protected readonly errorResolver: ErrorResolver
  protected readonly executionContext: ExecutionContext

  public readonly _messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  protected constructor(
    dependencies: PubSubConsumerDependencies,
    options: ConsumerOptionsType,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver
    this.consumerOverrides = options.consumerOverrides ?? {}
    this.deadLetterQueueOptions = options.deadLetterQueue
    this.maxRetryDuration = options.maxRetryDuration ?? DEFAULT_MAX_RETRY_DURATION
    this.executionContext = executionContext
    this.isDeduplicationEnabled = !!options.enableConsumerDeduplication

    this._messageSchemaContainer = this.resolveConsumerMessageSchemaContainer(options)
    this.handlerContainer = new HandlerContainer({
      messageHandlers: options.handlers,
      messageTypeField: options.messageTypeField,
    })
  }

  public async start(): Promise<void> {
    await this.init()

    if (!this.subscription) {
      throw new Error('Subscription not initialized after init()')
    }

    // Verify subscription exists before starting to listen
    const [subscriptionExists] = await this.subscription.exists()
    if (!subscriptionExists) {
      throw new Error(`Subscription ${this.subscriptionName} does not exist after init`)
    }

    this.isConsuming = true

    // Configure message handler
    this.subscription.on('message', async (message: PubSubMessage) => {
      await this.handleMessage(message)
    })

    // Configure error handler
    this.subscription.on('error', (error) => {
      this.handleError(error)
    })

    // Configure flow control if provided
    // @ts-expect-error - consumerOverrides may have flowControl
    if (this.consumerOverrides?.flowControl) {
      this.subscription.setOptions({
        // @ts-expect-error - flowControl is available
        flowControl: this.consumerOverrides.flowControl,
      })
    }
  }

  public override async close(): Promise<void> {
    this.isConsuming = false
    if (this.subscription) {
      await this.subscription.close()
    }
    await super.close()
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: message handling requires complex logic
  private async handleMessage(message: PubSubMessage): Promise<void> {
    if (!this.isConsuming) {
      // If we're shutting down, nack the message
      message.nack()
      return
    }

    const messageProcessingStartTimestamp = Date.now()

    try {
      // Deserialize message
      const deserializedPayload = deserializePubSubMessage(message, this.errorResolver)
      if (deserializedPayload.error) {
        this.handleMessageProcessed({
          message: deserializedPayload.error.message as unknown as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack() // Invalid messages should be removed
        return
      }

      // Retrieve offloaded payload if needed
      let messagePayload = deserializedPayload.result
      if (hasOffloadedPayload(message.attributes)) {
        const retrievalResult = await this.retrieveOffloadedMessagePayload(messagePayload)
        if (retrievalResult.error) {
          this.handleMessageProcessed({
            message: messagePayload as MessagePayloadType,
            processingResult: {
              status: 'error',
              errorReason: 'invalidMessage',
            },
            messageProcessingStartTimestamp,
            queueName: this.subscriptionName ?? this.topicName,
          })
          message.ack()
          return
        }
        messagePayload = retrievalResult.result
      }

      // Parse and validate message
      const resolvedMessage = this.resolveMessage(message)
      if ('error' in resolvedMessage) {
        this.handleMessageProcessed({
          message: resolvedMessage.error.message as unknown as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        return
      }

      const resolveSchemaResult = this.resolveSchema(
        resolvedMessage.result.body as MessagePayloadType,
      )
      if ('error' in resolveSchemaResult) {
        this.handleError(resolveSchemaResult.error)
        message.ack()
        return
      }

      const parseResult = parseMessage(
        resolvedMessage.result.body,
        resolveSchemaResult.result,
        this.errorResolver,
      )

      if ('error' in parseResult) {
        this.handleMessageProcessed({
          message: resolvedMessage.result.body as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        return
      }

      const validatedMessage = parseResult.result.parsedMessage as MessagePayloadType

      // Acquire lock for message processing
      const acquireLockResult = this.isDeduplicationEnabledForMessage(validatedMessage)
        ? await this.acquireLockForMessage(validatedMessage)
        : { result: noopReleasableLock }

      // Lock cannot be acquired as it is already being processed by another consumer.
      // We don't want to discard message yet as we don't know if the other consumer will be able to process it successfully.
      // We're re-queueing the message, so it can be processed later.
      if ('error' in acquireLockResult) {
        message.nack()
        return
      }

      // While the consumer was waiting for a lock to be acquired, the message might have been processed
      // by another consumer already, hence we need to check again if the message is not marked as duplicated.
      if (
        this.isDeduplicationEnabledForMessage(validatedMessage) &&
        (await this.isMessageDuplicated(validatedMessage, DeduplicationRequesterEnum.Consumer))
      ) {
        await acquireLockResult.result?.release()
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: { status: 'consumed', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        return
      }

      const releaseLock = acquireLockResult.result

      // @ts-expect-error
      const messageType = validatedMessage[this.messageTypeField]

      try {
        // Process message
        const processingResult = await this.internalProcessMessage(validatedMessage, messageType)

        if (processingResult.error === 'retryLater') {
          // Check retry duration
          if (this.isRetryDateExceeded(validatedMessage)) {
            this.handleMessageProcessed({
              message: validatedMessage,
              processingResult: {
                status: 'error',
                errorReason: 'retryLaterExceeded',
              },
              messageProcessingStartTimestamp,
              queueName: this.subscriptionName ?? this.topicName,
            })
            message.ack() // Remove from queue (should go to DLQ if configured)
          } else {
            this.handleMessageProcessed({
              message: validatedMessage,
              processingResult: { status: 'retryLater' },
              messageProcessingStartTimestamp,
              queueName: this.subscriptionName ?? this.topicName,
            })
            message.nack() // Retry later
          }
          await releaseLock.release()
          return
        }

        // Success
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: { status: 'consumed' },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        await releaseLock.release()
      } catch (error) {
        await releaseLock.release()
        this.handleError(error as Error)
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: {
            status: 'error',
            errorReason: 'handlerError',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.nack()
      }
    } catch (error) {
      this.handleError(error as Error)
      message.nack()
    }
  }

  private async internalProcessMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const preHandlerOutput = await this.processPrehandlers(message, messageType)
    const barrierResult = await this.preHandlerBarrier(message, messageType, preHandlerOutput)

    if (barrierResult.isPassing) {
      return this.processMessage(message, messageType, {
        preHandlerOutput,
        barrierOutput: barrierResult.output,
      })
    }

    return { error: 'retryLater' }
  }

  protected override resolveMessage(message: PubSubMessage) {
    const deserializedPayload = deserializePubSubMessage(message, this.errorResolver)
    if (deserializedPayload.error) {
      return deserializedPayload
    }

    return {
      result: {
        body: deserializedPayload.result,
        attributes: message.attributes,
      },
    }
  }

  protected override resolveSchema(messagePayload: MessagePayloadType) {
    return this._messageSchemaContainer.resolveSchema(messagePayload)
  }

  protected override processMessage(
    message: MessagePayloadType,
    messageType: string,
    // biome-ignore lint/suspicious/noExplicitAny: Expected
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return handler.handler(message, this.executionContext, preHandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return this.processPrehandlersInternal(handlerConfig.preHandlers, message)
  }

  protected override preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput, BarrierOutput>(
      messageType,
    )

    return this.preHandlerBarrierInternal(
      handler.preHandlerBarrier,
      message,
      this.executionContext,
      preHandlerOutput,
    )
  }

  protected override resolveNextFunction(
    preHandlers: Prehandler<MessagePayloadType, ExecutionContext, unknown>[],
    message: MessagePayloadType,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return this.resolveNextPreHandlerFunctionInternal(
      preHandlers,
      this.executionContext,
      message,
      index,
      preHandlerOutput,
      resolve,
      reject,
    )
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override isDeduplicationEnabledForMessage(message: MessagePayloadType): boolean {
    return this.isDeduplicationEnabled && super.isDeduplicationEnabledForMessage(message)
  }

  private isRetryDateExceeded(message: MessagePayloadType): boolean {
    // @ts-expect-error
    const timestamp = message.timestamp
    if (!timestamp) {
      return false
    }

    const messageTimestamp = new Date(timestamp).getTime()
    const now = Date.now()
    const elapsedSeconds = (now - messageTimestamp) / 1000

    return elapsedSeconds > this.maxRetryDuration
  }
}
