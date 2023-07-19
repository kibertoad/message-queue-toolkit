import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver, Either } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema, ZodType } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'
import type { Logger, TransactionObservabilityManager } from '../types/MessageQueueTypes'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: Logger
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

export type Deserializer<
  MessagePayloadType extends object,
  QueueEngineMessageType extends object,
> = (
  message: QueueEngineMessageType,
  type: ZodType<MessagePayloadType>,
  errorProcessor: ErrorResolver,
) => Either<MessageInvalidFormatError | MessageValidationError, MessagePayloadType>

export type NewQueueOptions<
  MessagePayloadType extends object,
  CreationConfigType extends object,
> = {
  messageSchema: ZodSchema<MessagePayloadType>
  messageTypeField: string
  locatorConfig?: never
  creationConfig: CreationConfigType
}

export type ExistingQueueOptions<
    MessagePayloadType extends object,
    QueueLocatorType extends object,
> = {
  messageSchema: ZodSchema<MessagePayloadType>
  messageTypeField: string
  locatorConfig: QueueLocatorType
  creationConfig?: never
}

export type CommonQueueLocator = {
  queueName: string
}

export abstract class AbstractQueueService<
  MessagePayloadType extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  QueueLocatorType extends object = CommonQueueLocator,
  OptionsType extends NewQueueOptions<
    MessagePayloadType,
    QueueConfiguration
  >
      |
      ExistingQueueOptions<
          MessagePayloadType,
          QueueLocatorType
      >

      = NewQueueOptions<MessagePayloadType, QueueConfiguration> | ExistingQueueOptions<MessagePayloadType, QueueLocatorType>,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly messageSchema: ZodSchema<MessagePayloadType>
  protected readonly logger: Logger
  protected readonly messageTypeField: string
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType

  constructor(
    { errorReporter, logger }: DependenciesType,
    { messageSchema, messageTypeField, creationConfig, locatorConfig }: OptionsType,
  ) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.messageSchema = messageSchema
    this.messageTypeField = messageTypeField
    this.creationConfig = creationConfig
    this.locatorConfig = locatorConfig
  }

  protected handleError(err: unknown) {
    const logObject = resolveGlobalErrorLogObject(err)
    this.logger.error(logObject)
    if (types.isNativeError(err)) {
      this.errorReporter.report({ error: err })
    }
  }

  public abstract close(): Promise<unknown>
}
