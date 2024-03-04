import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver, Either } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema, ZodType } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'
import type {
  Logger,
  TransactionObservabilityManager,
  MessageProcessingResult,
} from '../types/MessageQueueTypes'

import type { MessageHandlerConfig } from './HandlerContainer'
import type { HandlerSpy, PublicHandlerSpy, HandlerSpyParams } from './HandlerSpy'
import { resolveHandlerSpy } from './HandlerSpy'

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
  PrehandlerOutput,
> = NewQueueOptions<CreationConfigType> &
  MultiSchemaConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>

export type ExistingQueueOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  QueueLocatorType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = ExistingQueueOptions<QueueLocatorType> &
  MultiSchemaConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>

export type DeletionConfig = {
  deleteIfExists?: boolean
  waitForConfirmation?: boolean
  forceDeleteInProduction?: boolean
}

export type CommonQueueOptions = {
  messageTypeField: string
  messageIdField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
}

export type CommonCreationConfigType = {
  updateAttributesIfExists?: boolean
}

export type NewQueueOptions<CreationConfigType extends CommonCreationConfigType> = {
  locatorConfig?: never
  deletionConfig?: DeletionConfig
  creationConfig: CreationConfigType
} & CommonQueueOptions

export type ExistingQueueOptions<QueueLocatorType extends object> = {
  locatorConfig: QueueLocatorType
  deletionConfig?: DeletionConfig
  creationConfig?: never
} & CommonQueueOptions

export type MultiSchemaPublisherOptions<MessagePayloadSchemas extends object> = {
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
}

export type MultiSchemaConsumerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
> = {
  handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
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
  public readonly logger: Logger
  protected readonly messageTypeField: string
  protected readonly messageIdField: string
  protected readonly logMessages: boolean
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType
  protected readonly deletionConfig?: DeletionConfig
  protected readonly _handlerSpy?: HandlerSpy<MessagePayloadSchemas>

  get handlerSpy(): PublicHandlerSpy<MessagePayloadSchemas> {
    if (!this._handlerSpy) {
      throw new Error(
        'HandlerSpy was not instantiated, please pass `handlerSpy` parameter during queue service creation.',
      )
    }
    return this._handlerSpy
  }

  constructor({ errorReporter, logger }: DependenciesType, options: OptionsType) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.messageTypeField = options.messageTypeField
    this.messageIdField = options.messageIdField ?? 'id'
    this.creationConfig = options.creationConfig
    this.locatorConfig = options.locatorConfig
    this.deletionConfig = options.deletionConfig

    this.logMessages = options.logMessages ?? false
    this._handlerSpy = resolveHandlerSpy<MessagePayloadSchemas>(options)
  }

  protected abstract resolveSchema(
    message: MessagePayloadSchemas,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>>

  protected abstract resolveMessage(
    message: MessageEnvelopeType,
  ): Either<MessageInvalidFormatError | MessageValidationError, unknown>

  /**
   * Format message for logging
   */
  protected resolveMessageLog(message: MessagePayloadSchemas, _messageType: string): unknown {
    return message
  }

  /**
   * Log preformatted and potentially presanitized message payload
   */
  protected logMessage(messageLogEntry: unknown) {
    this.logger.debug(messageLogEntry)
  }

  protected logProcessedMessage(
    _message: MessagePayloadSchemas | null,
    processingResult: MessageProcessingResult,
    messageId?: string,
  ) {
    this.logger.debug(
      {
        processingResult,
        messageId,
      },
      `Finished processing message ${messageId ?? `(unknown id)`}`,
    )
  }

  protected handleError(err: unknown, context?: Record<string, unknown>) {
    const logObject = resolveGlobalErrorLogObject(err)
    if (logObject === 'string') {
      this.logger.error(context, logObject)
    } else if (typeof logObject === 'object') {
      this.logger.error({
        ...logObject,
        ...context,
      })
    }
    if (types.isNativeError(err)) {
      this.errorReporter.report({ error: err, context })
    }
  }

  protected handleMessageProcessed(
    message: MessagePayloadSchemas | null,
    processingResult: MessageProcessingResult,
    messageId?: string,
  ) {
    if (this._handlerSpy) {
      this._handlerSpy.addProcessedMessage(
        {
          message,
          processingResult,
        },
        messageId,
      )
    }
    if (this.logMessages) {
      // @ts-ignore
      const resolvedMessageId: string | undefined = message?.[this.messageIdField] ?? messageId

      this.logProcessedMessage(message, processingResult, resolvedMessageId)
    }
  }

  public abstract close(): Promise<unknown>
}
