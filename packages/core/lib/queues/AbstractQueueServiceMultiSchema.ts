import { types } from 'node:util'

import type { ErrorReporter } from '@lokalise/node-core'
import {resolveGlobalErrorLogObject} from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { Logger } from '../types/MessageQueueTypes'

import type { CommonQueueLocator, QueueDependencies } from './AbstractQueueService'
import {MessageHandlerConfig} from "./HandlerContainer";

export type MultiSchemaConsumerOptions<MessagePayloadSchemas extends object, ExecutionContext> = {
  handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]
}

export type NewQueueOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  CreationConfigType extends object,
> = {
  messageSchemas: ZodSchema<MessagePayloadSchemas>[]
  messageTypeField: string
  locatorConfig?: never
  creationConfig: CreationConfigType
}

export type ExistingQueueOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  QueueLocatorType extends object,
> = {
  messageSchemas: ZodSchema<MessagePayloadSchemas>[]
  messageTypeField: string
  locatorConfig: QueueLocatorType
  creationConfig?: never
}

export abstract class AbstractQueueServiceMultiSchema<
  MessagePayloadSchemas extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  QueueLocatorType extends object = CommonQueueLocator,
  OptionsType extends
    | NewQueueOptionsMultiSchema<MessagePayloadSchemas, QueueConfiguration>
    | ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, QueueLocatorType> =
    | NewQueueOptionsMultiSchema<MessagePayloadSchemas, QueueConfiguration>
    | ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, QueueLocatorType>,
> {
  protected readonly errorReporter: ErrorReporter
  protected readonly messageSchemas: Record<string, ZodSchema<MessagePayloadSchemas>>
  protected readonly logger: Logger
  protected readonly messageTypeField: string
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType

  constructor(
    { errorReporter, logger }: DependenciesType,
    { messageSchemas, messageTypeField, creationConfig, locatorConfig }: OptionsType,
  ) {
    this.errorReporter = errorReporter
    this.logger = logger

    this.messageSchemas = this.resolveSchemaMap(messageSchemas)
    this.messageTypeField = messageTypeField
    this.creationConfig = creationConfig
    this.locatorConfig = locatorConfig
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected resolveSchema(message: Record<string, any>): ZodSchema<MessagePayloadSchemas> {
    const schema = this.messageSchemas[message[this.messageTypeField]]
    if (!schema) {
      throw new Error(`Unsupported message type: ${message[this.messageTypeField]}`)
    }
    return schema
  }

  protected resolveSchemaMap(
    supportedSchemas: ZodSchema<MessagePayloadSchemas>[],
  ): Record<string, ZodSchema<MessagePayloadSchemas>> {
    return supportedSchemas.reduce(
      (acc, schema) => {
        // @ts-ignore
        acc[schema.shape.messageType.value] = schema
        return acc
      },
      {} as Record<string, ZodSchema<MessagePayloadSchemas>>,
    )
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
