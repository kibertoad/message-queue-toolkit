import type { Either } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

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

export type BarrierCallbackMultiConsumers<
  MessagePayloadSchema extends object,
  ExecutionContext,
  BarrierOutput,
> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
) => Promise<BarrierResult<BarrierOutput>>

export const defaultLogFormatter = <MessagePayloadSchema>(message: MessagePayloadSchema) => message

export type HandlerConfigOptions<
  MessagePayloadSchema extends object,
  ExecutionContext,
  BarrierOutput,
> = {
  messageLogFormatter?: LogFormatter<MessagePayloadSchema>
  preHandlerBarrier?: BarrierCallbackMultiConsumers<
    MessagePayloadSchema,
    ExecutionContext,
    BarrierOutput
  >
}

export class MessageHandlerConfig<
  const MessagePayloadSchema extends object,
  const ExecutionContext,
  const BarrierOutput = unknown,
> {
  public readonly schema: ZodSchema<MessagePayloadSchema>
  public readonly handler: Handler<MessagePayloadSchema, ExecutionContext, BarrierOutput>
  public readonly messageLogFormatter: LogFormatter<MessagePayloadSchema>
  public readonly preHandlerBarrier?: BarrierCallbackMultiConsumers<
    MessagePayloadSchema,
    ExecutionContext,
    BarrierOutput
  >

  constructor(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, BarrierOutput>,
    options?: HandlerConfigOptions<MessagePayloadSchema, ExecutionContext, BarrierOutput>,
  ) {
    this.schema = schema
    this.handler = handler
    this.messageLogFormatter = options?.messageLogFormatter ?? defaultLogFormatter
    this.preHandlerBarrier = options?.preHandlerBarrier
  }
}

export class MessageHandlerConfigBuilder<MessagePayloadSchemas extends object, ExecutionContext> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly configs: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, any>[]

  constructor() {
    this.configs = []
  }

  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, BarrierOutput>,
    options?: HandlerConfigOptions<MessagePayloadSchema, ExecutionContext, BarrierOutput>,
  ) {
    this.configs.push(
      new MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, BarrierOutput>(
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

export type Handler<MessagePayloadSchemas, ExecutionContext, BarrierOutput = undefined> = (
  message: MessagePayloadSchemas,
  context: ExecutionContext,
  barrierOutput: BarrierOutput,
) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<MessagePayloadSchemas extends object, ExecutionContext> = {
  messageHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, unknown>[]
  messageTypeField: string
}

export class HandlerContainer<MessagePayloadSchemas extends object, ExecutionContext> {
  private readonly messageHandlers: Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, unknown>
  >
  private readonly messageTypeField: string

  constructor(options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext>) {
    this.messageTypeField = options.messageTypeField
    this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
  }

  public resolveHandler<BarrierResult>(
    messageType: string,
  ): MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, BarrierResult> {
    const handler = this.messageHandlers[messageType]
    if (!handler) {
      throw new Error(`Unsupported message type: ${messageType}`)
    }
    // @ts-ignore
    return handler
  }

  private resolveHandlerMap(
    supportedHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, unknown>[],
  ): Record<string, MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, unknown>> {
    return supportedHandlers.reduce(
      (acc, entry) => {
        // @ts-ignore
        const messageType = entry.schema.shape[this.messageTypeField].value
        acc[messageType] = entry
        return acc
      },
      {} as Record<string, MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, unknown>>,
    )
  }
}
