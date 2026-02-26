import {
  type CommonLogger,
  type ErrorReporter,
  isError,
  resolveGlobalErrorLogObject,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { MakeRequired, MayOmit } from '@lokalise/universal-ts-utils/node'
import {
  type HandlerSpy,
  type HandlerSpyParams,
  type MessageMetricsManager,
  type MessageProcessingResult,
  type PublicHandlerSpy,
  resolveHandlerSpy,
  TYPE_NOT_RESOLVED,
} from '@message-queue-toolkit/core'
import type { BaseOptions } from '@platformatic/kafka'
import type {
  DeserializedMessage,
  KafkaConfig,
  KafkaDependencies,
  RequestContext,
  SupportedMessageValues,
  TopicConfig,
} from './types.ts'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
  messageIdField?: string
  /**
   * The field in the message headers that contains the request ID.
   * This is used to correlate logs and transactions with the request.
   * Defaults to 'x-request-id'.
   */
  headerRequestIdField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
} & Omit<BaseOptions, keyof KafkaConfig> // Exclude properties that are already in KafkaConfig

export type ProcessedMessage<TopicsConfig extends TopicConfig[]> = MayOmit<
  Pick<DeserializedMessage<SupportedMessageValues<TopicsConfig>>, 'topic' | 'value' | 'timestamp'>,
  'timestamp'
>

export abstract class AbstractKafkaService<
  TopicsConfig extends TopicConfig[],
  KafkaOptions extends BaseKafkaOptions,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly logger: CommonLogger
  protected readonly messageMetricsManager?: MessageMetricsManager<
    SupportedMessageValues<TopicsConfig>
  >

  protected readonly options: MakeRequired<KafkaOptions, 'messageIdField'>
  protected readonly _handlerSpy?: HandlerSpy<SupportedMessageValues<TopicsConfig>>

  constructor(dependencies: KafkaDependencies, options: KafkaOptions) {
    this.logger = dependencies.logger
    this.errorReporter = dependencies.errorReporter
    this.messageMetricsManager = dependencies.messageMetricsManager
    this.options = { ...options, messageIdField: options.messageIdField ?? 'id' }

    this._handlerSpy = resolveHandlerSpy(options)
  }

  abstract init(): Promise<void>
  abstract close(): Promise<void>

  get handlerSpy(): PublicHandlerSpy<SupportedMessageValues<TopicsConfig>> {
    if (this._handlerSpy) return this._handlerSpy

    throw new Error(
      'HandlerSpy was not instantiated, please pass `handlerSpy` parameter during creation.',
    )
  }

  protected resolveMessageId(message: SupportedMessageValues<TopicsConfig>): string | undefined {
    // @ts-expect-error
    return message[this.options.messageIdField] as string | undefined
  }

  protected resolveHeaderRequestIdField(): string {
    return this.options.headerRequestIdField ?? 'x-request-id'
  }

  protected handleMessageProcessed(params: {
    message: ProcessedMessage<TopicsConfig>
    processingResult: MessageProcessingResult
    messageProcessingStartTimestamp: number
  }) {
    const { message, processingResult } = params
    const messageId = this.resolveMessageId(message.value)

    // Kafka doesn't have unified message type resolution yet, use TYPE_NOT_RESOLVED
    this._handlerSpy?.addProcessedMessage(
      { message: message.value, processingResult },
      messageId,
      TYPE_NOT_RESOLVED,
    )

    if (this.options.logMessages) {
      this.logger.debug(
        {
          message: stringValueSerializer(message.value),
          topic: message.topic,
          processingResult,
          messageId,
        },
        `Finished processing message ${messageId}`,
      )
    }

    if (this.messageMetricsManager) {
      this.messageMetricsManager.registerProcessedMessage({
        message: message.value,
        processingResult,
        queueName: message.topic,
        messageId: messageId ?? 'unknown',
        messageType: 'unknown',
        messageTimestamp: message.timestamp ? Number(message.timestamp) : undefined,
        messageProcessingStartTimestamp: params.messageProcessingStartTimestamp,
        messageProcessingEndTimestamp: Date.now(),
      })
    }
  }

  protected handlerError(error: unknown, context: Record<string, unknown> = {}): void {
    this.logger.error({ ...resolveGlobalErrorLogObject(error), ...context })
    if (isError(error))
      this.errorReporter.report({
        error,
        context: context,
      })
  }
}
