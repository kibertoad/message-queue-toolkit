import { types } from 'node:util'

import type { ErrorReporter, ErrorResolver, Either } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { ZodSchema, ZodType } from 'zod'

import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'
import type { Logger, MessageProcessingResult } from '../types/MessageQueueTypes'
import type { DeletionConfig, QueueDependencies, QueueOptions } from '../types/queueOptionsTypes'
import { toDatePreprocessor } from '../utils/toDateProcessor'

import type {
  BarrierCallback,
  BarrierResult,
  Prehandler,
  PrehandlerResult,
  PreHandlingOutputs,
} from './HandlerContainer'
import type { HandlerSpy, PublicHandlerSpy } from './HandlerSpy'
import { resolveHandlerSpy } from './HandlerSpy'

export type Deserializer<MessagePayloadType extends object> = (
  message: unknown,
  type: ZodType<MessagePayloadType>,
  errorProcessor: ErrorResolver,
) => Either<MessageInvalidFormatError | MessageValidationError, MessagePayloadType>

type CommonQueueLocator = {
  queueName: string
}

export abstract class AbstractQueueService<
  MessagePayloadSchemas extends object,
  MessageEnvelopeType extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  QueueLocatorType extends object = CommonQueueLocator,
  OptionsType extends QueueOptions<QueueConfiguration, QueueLocatorType> = QueueOptions<
    QueueConfiguration,
    QueueLocatorType
  >,
  ExecutionContext = undefined,
  PrehandlerOutput = undefined,
> {
  protected readonly errorReporter: ErrorReporter
  public readonly logger: Logger
  protected readonly messageIdField: string
  protected readonly messageTypeField: string
  protected readonly messageTimestampField: string
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

    this.messageIdField = options.messageIdField ?? 'id'
    this.messageTypeField = options.messageTypeField
    this.messageTimestampField = options.messageTimestampField ?? 'timestamp'
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

  protected processPrehandlersInternal(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    message: MessagePayloadSchemas,
  ) {
    if (preHandlers.length === 0) {
      return Promise.resolve({} as PrehandlerOutput)
    }

    return new Promise<PrehandlerOutput>((resolve, reject) => {
      try {
        const preHandlerOutput = {} as PrehandlerOutput
        const next = this.resolveNextFunction(
          preHandlers,
          message,
          0,
          preHandlerOutput,
          resolve,
          reject,
        )
        next({ result: 'success' })
      } catch (err) {
        reject(err as Error)
      }
    })
  }

  protected async preHandlerBarrierInternal<BarrierOutput>(
    barrier:
      | BarrierCallback<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput, BarrierOutput>
      | undefined,
    message: MessagePayloadSchemas,
    executionContext: ExecutionContext,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    if (!barrier) {
      // @ts-ignore
      return {
        isPassing: true,
        output: undefined,
      }
    }

    // @ts-ignore
    return await barrier(message, executionContext, preHandlerOutput)
  }

  protected tryToExtractTimestamp(message: MessagePayloadSchemas): Date | undefined {
    // @ts-ignore
    if (this.messageTimestampField in message) {
      // @ts-ignore
      const res = toDatePreprocessor(message[this.messageTimestampField])

      if (!(res instanceof Date)) {
        throw new Error(`${this.messageTimestampField} invalid type`)
      }
    }

    return undefined
  }

  protected abstract resolveNextFunction(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    message: MessagePayloadSchemas,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ): (preHandlerResult: PrehandlerResult) => void

  // eslint-disable-next-line max-params
  protected resolveNextPreHandlerFunctionInternal(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    executionContext: ExecutionContext,
    message: MessagePayloadSchemas,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ): (preHandlerResult: PrehandlerResult) => void {
    return (preHandlerResult: PrehandlerResult) => {
      if (preHandlerResult.error) {
        reject(preHandlerResult.error)
      }

      if (preHandlers.length < index + 1) {
        resolve(preHandlerOutput)
      } else {
        // eslint-disable-next-line @typescript-eslint/no-unsafe-call
        preHandlers[index](
          message,
          executionContext,
          // @ts-ignore
          preHandlerOutput,
          this.resolveNextPreHandlerFunctionInternal(
            preHandlers,
            executionContext,
            message,
            index + 1,
            preHandlerOutput,
            resolve,
            reject,
          ),
        )
      }
    }
  }

  protected abstract processPrehandlers(
    message: MessagePayloadSchemas,
    messageType: string,
  ): Promise<PrehandlerOutput>

  protected abstract preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadSchemas,
    messageType: string,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>>

  protected abstract processMessage(
    message: MessagePayloadSchemas,
    messageType: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>>

  public abstract close(): Promise<unknown>
}
