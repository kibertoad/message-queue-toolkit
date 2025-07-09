import { types } from 'node:util'
import {
  type CommonLogger,
  type ErrorReporter,
  resolveGlobalErrorLogObject,
  stringValueSerializer,
} from '@lokalise/node-core'
import {
  type HandlerSpy,
  type HandlerSpyParams,
  type MessageMetricsManager,
  type MessageProcessingResult,
  type PublicHandlerSpy,
  resolveHandlerSpy,
} from '@message-queue-toolkit/core'
import type { BaseOptions } from '@platformatic/kafka'
import type {
  KafkaConfig,
  KafkaDependencies,
  SupportedMessageValues,
  TopicConfig,
} from './types.ts'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
  messageTypeField?: string
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

export abstract class AbstractKafkaService<
  TopicsConfig extends TopicConfig[],
  KafkaOptions extends BaseKafkaOptions,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly logger: CommonLogger
  protected readonly messageMetricsManager?: MessageMetricsManager<
    SupportedMessageValues<TopicsConfig>
  >

  protected readonly options: KafkaOptions
  protected readonly _handlerSpy?: HandlerSpy<SupportedMessageValues<TopicsConfig>>

  constructor(dependencies: KafkaDependencies, options: KafkaOptions) {
    this.logger = dependencies.logger
    this.errorReporter = dependencies.errorReporter
    this.messageMetricsManager = dependencies.messageMetricsManager
    this.options = options

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

  protected resolveMessageType(message: SupportedMessageValues<TopicsConfig>): string | undefined {
    if (!this.options.messageTypeField) return undefined
    return message[this.options.messageTypeField]
  }

  protected resolveMessageId(message: SupportedMessageValues<TopicsConfig>): string | undefined {
    if (!this.options.messageIdField) return undefined
    return message[this.options.messageIdField]
  }

  protected resolveHeaderRequestIdField(): string {
    return this.options.headerRequestIdField ?? 'x-request-id'
  }

  protected handleMessageProcessed(params: {
    message: SupportedMessageValues<TopicsConfig>
    processingResult: MessageProcessingResult
    messageProcessingStartTimestamp: number
    topic: string
  }) {
    const { message, processingResult, topic } = params
    const messageId = this.resolveMessageId(message) ?? 'unknown'
    const messageType = this.resolveMessageType(message) ?? 'unknown'

    this._handlerSpy?.addProcessedMessage({ message, processingResult }, messageId)

    if (this.options.logMessages) {
      this.logger.debug(
        {
          message: stringValueSerializer(message),
          topic,
          processingResult,
          messageId,
          messageType,
        },
        `Finished processing message ${messageId}`,
      )
    }

    if (this.messageMetricsManager) {
      this.messageMetricsManager.registerProcessedMessage({
        message,
        processingResult,
        queueName: topic,
        messageId,
        messageType,
        messageTimestamp: message.timestamp, // TODO: which is the format of this?
        messageProcessingStartTimestamp: params.messageProcessingStartTimestamp,
        messageProcessingEndTimestamp: Date.now(),
      })
    }
  }

  protected handlerError(error: unknown, context: Record<string, unknown> = {}): void {
    this.logger.error({ ...resolveGlobalErrorLogObject(error), ...context })
    if (types.isNativeError(error)) this.errorReporter.report({ error, context })
  }
}
