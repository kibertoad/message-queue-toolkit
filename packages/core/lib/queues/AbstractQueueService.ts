import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver, Either } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema, ZodType } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'
import type { Logger, TransactionObservabilityManager } from '../types/MessageQueueTypes'

import type { MessageHandlerConfig } from './HandlerContainer'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: Logger
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

export type Deserializer<MessagePayloadType extends object> = (
  message: unknown,
  type: ZodType<MessagePayloadType>,
  errorProcessor: ErrorResolver,
) => Either<MessageInvalidFormatError | MessageValidationError, MessagePayloadType>

export type NewQueueOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  CreationConfigType extends object,
  ExecutionContext,
> = NewQueueOptions<CreationConfigType> &
  MultiSchemaConsumerOptions<MessagePayloadSchemas, ExecutionContext>

export type ExistingQueueOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  QueueLocatorType extends object,
  ExecutionContext,
> = ExistingQueueOptions<QueueLocatorType> &
  MultiSchemaConsumerOptions<MessagePayloadSchemas, ExecutionContext>

export type DeletionConfig = {
  deleteIfExists?: boolean
  forceDeleteInProduction?: boolean
}

export type NewQueueOptions<CreationConfigType extends object> = {
  messageTypeField: string
  locatorConfig?: never
  deletionConfig?: DeletionConfig
  creationConfig: CreationConfigType
}

export type ExistingQueueOptions<QueueLocatorType extends object> = {
  messageTypeField: string
  locatorConfig: QueueLocatorType
  deletionConfig?: DeletionConfig
  creationConfig?: never
}

export type MultiSchemaPublisherOptions<MessagePayloadSchemas extends object> = {
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
}

export type MultiSchemaConsumerOptions<MessagePayloadSchemas extends object, ExecutionContext> = {
  handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]
}

export type MonoSchemaQueueOptions<MessagePayloadType extends object> = {
  messageSchema: ZodSchema<MessagePayloadType>
}

export type CommonQueueLocator = {
  queueName: string
}

export abstract class AbstractQueueService<
  MessagePayloadSchemas extends object,
  MessageEnvelopeType extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  QueueLocatorType extends object = CommonQueueLocator,
  OptionsType extends
    | NewQueueOptions<QueueConfiguration>
    | ExistingQueueOptions<QueueLocatorType> =
    | NewQueueOptions<QueueConfiguration>
    | ExistingQueueOptions<QueueLocatorType>,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly logger: Logger
  protected readonly messageTypeField: string
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType
  protected readonly deletionConfig?: DeletionConfig

  constructor({ errorReporter, logger }: DependenciesType, options: OptionsType) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.messageTypeField = options.messageTypeField
    this.creationConfig = options.creationConfig
    this.locatorConfig = options.locatorConfig
    this.deletionConfig = options.deletionConfig
  }

  protected abstract resolveSchema(
    message: MessagePayloadSchemas,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>>
  protected abstract resolveMessage(
    message: MessageEnvelopeType,
  ): Either<MessageInvalidFormatError | MessageValidationError, unknown>

  protected handleError(err: unknown) {
    const logObject = resolveGlobalErrorLogObject(err)
    this.logger.error(logObject)
    if (types.isNativeError(err)) {
      this.errorReporter.report({ error: err })
    }
  }

  public abstract close(): Promise<unknown>
}
