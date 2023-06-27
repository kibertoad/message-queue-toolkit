import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import { Either, InternalError, resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { Logger, TransactionObservabilityManager } from '../types/MessageQueueTypes'
import { ZodType } from 'zod'
import { SnsMessageInvalidFormat, SnsValidationError } from '@message-queue-toolkit/sns'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: Logger
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

export type Deserializer = <T extends object>(
  message: unknown,
  type: ZodType<T>,
  errorProcessor: ErrorResolver,
) => Either<InternalError, T>

export type QueueOptions<MessagePayloadType extends object, QueueConfiguration extends object> = {
  messageSchema: ZodSchema<MessagePayloadType>
  messageTypeField: string
  queueName: string
  queueConfiguration: QueueConfiguration
  deserializer?: <T extends object>(
    message: any,
    type: ZodType<T>,
    errorProcessor: ErrorResolver,
  ) => Either<InternalError, T>
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
