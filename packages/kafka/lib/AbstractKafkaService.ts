import type { CommonLogger, ErrorReporter } from '@lokalise/node-core'
import {
  type HandlerSpy,
  type HandlerSpyParams,
  resolveHandlerSpy,
} from '@message-queue-toolkit/core'
import type { BaseOptions } from '@platformatic/kafka'
import type { KafkaConfig, KafkaDependencies, KafkaTopicCreatorLocator } from './types.js'

export type BaseKafkaOptions = {
  kafka: KafkaConfig
  messageTypeField?: string
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

  protected readonly options: KafkaOptions
  protected readonly _handlerSpy?: HandlerSpy<MessagePayload>

  constructor(dependencies: KafkaDependencies, options: KafkaOptions) {
    this.logger = dependencies.logger
    this.errorReporter = dependencies.errorReporter
    this.options = options

    this._handlerSpy = resolveHandlerSpy(options)
  }

  abstract init(): Promise<void>
  abstract close(): Promise<void>
}
