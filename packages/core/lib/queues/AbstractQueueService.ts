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

export type QueueOptions<MessagePayloadType extends object, QueueConfiguration extends object> = {
  messageSchema: ZodSchema<MessagePayloadType>
  messageTypeField: string
  queueName: string
  queueConfiguration: QueueConfiguration
}

export abstract class AbstractQueueService<
  MessagePayloadType extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  OptionsType extends QueueOptions<MessagePayloadType, QueueConfiguration> = QueueOptions<
    MessagePayloadType,
    QueueConfiguration
  >,
> {
  protected readonly queueName: string
  protected readonly errorReporter: ErrorReporter
  protected readonly messageSchema: ZodSchema<MessagePayloadType>
  protected readonly logger: Logger
  protected readonly messageTypeField: string
  protected readonly queueConfiguration: QueueConfiguration

  constructor(
    { errorReporter, logger }: DependenciesType,
    { messageSchema, messageTypeField, queueName, queueConfiguration }: OptionsType,
  ) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.queueName = queueName
    this.messageSchema = messageSchema
    this.messageTypeField = messageTypeField
    this.queueConfiguration = queueConfiguration
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
