import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { Logger, TransactionObservabilityManager } from '../types/MessageQueueTypes'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: Logger
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

export type QueueOptions<MessagePayloadType extends object> = {
  messageSchema: ZodSchema<MessagePayloadType>
  messageTypeField: string
  queueName: string
}

export abstract class AbstractQueueService<
  MessagePayloadType extends object,
  DependenciesType extends QueueDependencies,
  OptionsType extends QueueOptions<MessagePayloadType> = QueueOptions<MessagePayloadType>,
> {
  protected readonly queueName: string
  protected readonly errorReporter: ErrorReporter
  protected readonly messageSchema: ZodSchema<MessagePayloadType>
  protected readonly logger: Logger
  protected readonly messageTypeField: string

  constructor(
    { errorReporter, logger }: DependenciesType,
    { messageSchema, queueName, messageTypeField }: OptionsType,
  ) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.messageSchema = messageSchema
    this.queueName = queueName
    this.messageTypeField = messageTypeField
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
