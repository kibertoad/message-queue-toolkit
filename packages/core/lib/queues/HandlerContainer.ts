import type { Either } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { DoNotProcessMessageError } from '../errors/DoNotProcessError'
import type { RetryMessageLaterError } from '../errors/RetryMessageLaterError'

export type PrehandlingOutputs<PrehandlerOutput = undefined, BarrierOutput = undefined> = {
  prehandlerOutput: PrehandlerOutput
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

export type BarrierCallbackMultiConsumers<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput,
> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  prehandlerOutput: PrehandlerOutput,
) => Promise<BarrierResult<BarrierOutput>>

export type Prehandler<MessagePayloadSchema extends object, ExecutionContext, PrehandlerOutput> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  prehandlerOutput: Partial<PrehandlerOutput>,
  next: (result: PrehandlerResult) => void,
) => void

export const defaultLogFormatter = <MessagePayloadSchema>(message: MessagePayloadSchema) => message

export type HandlerConfigOptions<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutput = undefined,
> = {
  messageLogFormatter?: LogFormatter<MessagePayloadSchema>
  preHandlerBarrier?: BarrierCallbackMultiConsumers<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  prehandlers?: Prehandler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput>[]
}

export class MessageHandlerConfig<
  const MessagePayloadSchema extends object,
  const ExecutionContext,
  const PrehandlerOutput = undefined,
  const BarrierOutput = undefined,
> {
  public readonly schema: ZodSchema<MessagePayloadSchema>
  public readonly handler: Handler<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly messageLogFormatter: LogFormatter<MessagePayloadSchema>
  public readonly preHandlerBarrier?: BarrierCallbackMultiConsumers<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly prehandlers?: Prehandler<
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
  ) {
    this.schema = schema
    this.handler = handler
    this.messageLogFormatter = options?.messageLogFormatter ?? defaultLogFormatter
    this.preHandlerBarrier = options?.preHandlerBarrier
    this.prehandlers = options?.prehandlers
  }
}

export class MessageHandlerConfigBuilder<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutputs = undefined,
> {
  private readonly configs: MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutputs
  >[]

  constructor() {
    this.configs = []
  }

  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
  ) {
    this.configs.push(
      // @ts-ignore
      new MessageHandlerConfig<
        MessagePayloadSchemas,
        ExecutionContext,
        PrehandlerOutput,
        BarrierOutput
      >(
        schema,
        // @ts-ignore
        handler,
        options,
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
  prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, BarrierOutput>,
) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutput = undefined,
> = {
  messageHandlers: MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >[]
  messageTypeField: string
}

export class HandlerContainer<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutputs = undefined,
> {
  private readonly messageHandlers: Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput, BarrierOutputs>
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
    // @ts-ignore
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return handler
  }

  private resolveHandlerMap(
    supportedHandlers: MessageHandlerConfig<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput
    >[],
  ): Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput, BarrierOutputs>
  > {
    return supportedHandlers.reduce(
      (acc, entry) => {
        // @ts-ignore
        const messageType = entry.schema.shape[this.messageTypeField].value
        // @ts-ignore
        acc[messageType] = entry
        return acc
      },
      {} as Record<
        string,
        MessageHandlerConfig<
          MessagePayloadSchemas,
          ExecutionContext,
          PrehandlerOutput,
          BarrierOutputs
        >
      >,
    )
  }
}
