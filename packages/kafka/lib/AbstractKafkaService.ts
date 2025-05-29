import { type CommonLogger, type ErrorReporter, stringValueSerializer } from '@lokalise/node-core'
import {
  type HandlerSpy,
  type HandlerSpyParams,
  type MessageProcessingResult,
  type PublicHandlerSpy,
  resolveHandlerSpy,
} from '@message-queue-toolkit/core'
import type { BaseOptions } from '@platformatic/kafka'
import type { KafkaConfig, KafkaDependencies, KafkaTopicCreatorLocator } from './types.js'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
  messageTypeField: string
  messageIdField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
} & (
  | {
      creationConfig: KafkaTopicCreatorLocator
      locatorConfig?: never
    }
  | {
      creationConfig?: never
      locatorConfig: KafkaTopicCreatorLocator
    }
) &
  Omit<BaseOptions, keyof KafkaConfig | 'autocreateTopics'> // Exclude properties that are already in KafkaConfig

export abstract class AbstractKafkaService<
  MessagePayload extends object,
  KafkaOptions extends BaseKafkaOptions = BaseKafkaOptions,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly logger: CommonLogger

  protected readonly topics: string[]
  protected readonly autocreateTopics: boolean
  protected readonly options: KafkaOptions
  protected readonly _handlerSpy?: HandlerSpy<MessagePayload>

  constructor(dependencies: KafkaDependencies, options: KafkaOptions) {
    this.logger = dependencies.logger
    this.errorReporter = dependencies.errorReporter
    this.options = options

    const { creationConfig, locatorConfig } = options
    const topic =
      creationConfig?.topics ??
      creationConfig?.topic ??
      locatorConfig?.topics ??
      locatorConfig?.topic
    if (!topic) throw new Error('Topic must be defined in creationConfig or locatorConfig')

    this.topics = Array.isArray(topic) ? topic : [topic]
    this.autocreateTopics = !!creationConfig
    this._handlerSpy = resolveHandlerSpy(options)
  }

  abstract init(): Promise<void>
  abstract close(): Promise<void>

  get handlerSpy(): PublicHandlerSpy<MessagePayload> {
    if (this._handlerSpy) return this._handlerSpy

    throw new Error(
      'HandlerSpy was not instantiated, please pass `handlerSpy` parameter during creation.',
    )
  }

  protected resolveMessageType(message: MessagePayload | null | undefined): string | undefined {
    // @ts-expect-error
    return message[this.options.messageTypeField]
  }

  protected resolveMessageId(message: MessagePayload | null | undefined): string | undefined {
    // @ts-expect-error
    return message[this.options.messageIdField]
  }

  protected handleMessageProcessed(params: {
    message: MessagePayload | null
    processingResult: MessageProcessingResult
    topics: string | string[]
  }) {
    const { message, processingResult, topics } = params
    const messageId = this.resolveMessageId(message)

    this._handlerSpy?.addProcessedMessage({ message, processingResult }, messageId)

    if (this.options.logMessages) {
      this.logger.debug(
        { message: stringValueSerializer(message), topics, processingResult },
        `Finished processing message ${messageId ?? 'unknown'}`,
      )
    }
  }
}
