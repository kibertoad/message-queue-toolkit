import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import { isCommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v4'

import type { DoNotProcessMessageError } from '../errors/DoNotProcessError.ts'
import type { RetryMessageLaterError } from '../errors/RetryMessageLaterError.ts'

export type PreHandlingOutputs<PrehandlerOutput = undefined, BarrierOutput = undefined> = {
  preHandlerOutput: PrehandlerOutput
  barrierOutput: BarrierOutput
}

export type LogFormatter<MessagePayloadSchema> = (message: MessagePayloadSchema) => unknown

export type BarrierResult<BarrierOutput> =
  | BarrierResultPositive<BarrierOutput>
  | BarrierResultNegative

export type BarrierResultPositive<BarrierOutput> = {
  isPassing: true
  output: BarrierOutput
}

export type BarrierResultNegative = {
  isPassing: false
  output?: never
}

export type PrehandlerResult = Either<DoNotProcessMessageError | RetryMessageLaterError, 'success'>

export type BarrierCallback<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput,
> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  preHandlerOutput: PrehandlerOutput,
) => Promise<BarrierResult<BarrierOutput>>

export type Prehandler<MessagePayloadSchema extends object, ExecutionContext, PrehandlerOutput> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  preHandlerOutput: Partial<PrehandlerOutput>,
  next: (result: PrehandlerResult) => void,
) => void

export const defaultLogFormatter = <MessagePayloadSchema>(message: MessagePayloadSchema) => message

export type HandlerConfigOptions<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput,
> = {
  messageLogFormatter?: LogFormatter<MessagePayloadSchema>
  preHandlerBarrier?: BarrierCallback<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  preHandlers?: Prehandler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput>[]
}

export class MessageHandlerConfig<
  const MessagePayloadSchema extends object,
  const ExecutionContext,
  const PrehandlerOutput = undefined,
  const BarrierOutput = unknown,
> {
  public readonly schema: ZodSchema<MessagePayloadSchema>
  public readonly definition?: CommonEventDefinition
  public readonly handler: Handler<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly messageLogFormatter: LogFormatter<MessagePayloadSchema>
  public readonly preHandlerBarrier?: BarrierCallback<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly preHandlers: Prehandler<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput
  >[]

  constructor(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
    eventDefinition?: CommonEventDefinition,
  ) {
    this.schema = schema
    this.definition = eventDefinition
    this.handler = handler
    this.messageLogFormatter = options?.messageLogFormatter ?? defaultLogFormatter
    this.preHandlerBarrier = options?.preHandlerBarrier
    this.preHandlers = options?.preHandlers ?? []
  }
}

export class MessageHandlerConfigBuilder<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> {
  private readonly configs: MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    any
  >[]

  constructor() {
    this.configs = []
  }

  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    schema: ZodSchema<MessagePayloadSchema> | CommonEventDefinition,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
  ) {
    const resolvedSchema: ZodSchema<MessagePayloadSchema> = isCommonEventDefinition(schema)
      ? // @ts-ignore
        (schema.consumerSchema as ZodSchema<MessagePayloadSchema>)
      : schema
    const definition = isCommonEventDefinition(schema) ? schema : undefined

    this.configs.push(
      new MessageHandlerConfig<
        MessagePayloadSchemas,
        ExecutionContext,
        PrehandlerOutput,
        BarrierOutput
      >(
        resolvedSchema,
        // @ts-expect-error
        handler,
        options,
        definition,
      ),
    )
    return this
  }

  build() {
    return this.configs
  }
}

export type Handler<
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutput = undefined,
> = (
  message: MessagePayloadSchemas,
  context: ExecutionContext,
  preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, BarrierOutput>,
  definition?: CommonEventDefinition,
) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> = {
  messageHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
  messageTypeField: string
}

export class HandlerContainer<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> {
  // biome-ignore  lint/correctness/noUnusedPrivateClassMembers: this is actually used
  private readonly messageHandlers: Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
  >
  private readonly messageTypeField: string

  constructor(
    options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
  ) {
    this.messageTypeField = options.messageTypeField
    this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
  }

  public resolveHandler<PrehandlerOutput = undefined, BarrierOutput = undefined>(
    messageType: string,
  ): MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  > {
    const handler = this.messageHandlers[messageType]
    if (!handler) {
      throw new Error(`Unsupported message type: ${messageType}`)
    }
    // @ts-expect-error
    return handler
  }

  // biome-ignore  lint/correctness/noUnusedPrivateClassMembers: this is actually used
  private resolveHandlerMap(
    supportedHandlers: MessageHandlerConfig<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput
    >[],
  ): Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
  > {
    return supportedHandlers.reduce(
      (acc, entry) => {
        // @ts-expect-error
        const messageType = entry.schema.shape[this.messageTypeField].value
        acc[messageType] = entry
        return acc
      },
      {} as Record<
        string,
        MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
      >,
    )
  }
}
