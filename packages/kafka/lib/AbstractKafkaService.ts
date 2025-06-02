import { types } from 'node:util'
import {
  type CommonLogger,
  type ErrorReporter,
  resolveGlobalErrorLogObject,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { HandlerSpy, HandlerSpyParams } from '@message-queue-toolkit/core'
import {
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
} from './types.js'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
  messageTypeField: string
  messageIdField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
} & Omit<BaseOptions, keyof KafkaConfig> // Exclude properties that are already in KafkaConfig

export abstract class AbstractKafkaService<
  TopicsConfig extends TopicConfig[],
  KafkaOptions extends BaseKafkaOptions,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly logger: CommonLogger

  protected readonly options: KafkaOptions
  protected readonly _handlerSpy?: HandlerSpy<SupportedMessageValues<TopicsConfig>>

  constructor(dependencies: KafkaDependencies, options: KafkaOptions) {
    this.logger = dependencies.logger
    this.errorReporter = dependencies.errorReporter
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

  protected resolveMessageType(
    message: SupportedMessageValues<TopicsConfig> | null | undefined,
  ): string | undefined {
    // @ts-expect-error
    return message[this.options.messageTypeField]
  }

  protected resolveMessageId(
    message: SupportedMessageValues<TopicsConfig> | null | undefined,
  ): string | undefined {
    // @ts-expect-error
    return message[this.options.messageIdField]
  }

  protected handleMessageProcessed(params: {
    message: SupportedMessageValues<TopicsConfig> | null
    processingResult: MessageProcessingResult
    topic: string
  }) {
    const { message, processingResult, topic } = params
    const messageId = this.resolveMessageId(message)

    this._handlerSpy?.addProcessedMessage({ message, processingResult }, messageId)

    if (this.options.logMessages) {
      this.logger.debug(
        {
          message: stringValueSerializer(message),
          topic,
          processingResult,
          messageId,
          messageType: this.resolveMessageType(message),
        },
        `Finished processing message ${messageId}`,
      )
    }
  }

  protected handlerError(error: unknown, context: Record<string, unknown> = {}): void {
    this.logger.error({ ...resolveGlobalErrorLogObject(error), ...context })
    if (types.isNativeError(error)) this.errorReporter.report({ error, context })
  }
}
