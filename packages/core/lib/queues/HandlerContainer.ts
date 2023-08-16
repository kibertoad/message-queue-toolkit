import type { Either } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

export type LogFormatter<MessagePayloadSchema> = (message: MessagePayloadSchema) => unknown

export const defaultLogFormatter = <MessagePayloadSchema>(message: MessagePayloadSchema) => message

export type HandlerConfigOptions<MessagePayloadSchema> = {
  messageLogFormatter?: LogFormatter<MessagePayloadSchema>
}

export class MessageHandlerConfig<const MessagePayloadSchema, const ExecutionContext> {
  public readonly schema: ZodSchema<MessagePayloadSchema>
  public readonly messageLogFormatter: LogFormatter<MessagePayloadSchema>
  public readonly handler: Handler<MessagePayloadSchema, ExecutionContext>

  constructor(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext>,
    options?: HandlerConfigOptions<MessagePayloadSchema>,
  ) {
    this.schema = schema
    this.handler = handler
    this.messageLogFormatter = options?.messageLogFormatter ?? defaultLogFormatter
  }
}

export class MessageHandlerConfigBuilder<MessagePayloadSchemas, ExecutionContext> {
  private readonly configs: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]

  constructor() {
    this.configs = []
  }

  addConfig<MessagePayloadSchema extends MessagePayloadSchemas>(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext>,
    options?: HandlerConfigOptions<MessagePayloadSchema>,
  ) {
    // @ts-ignore
    this.configs.push(new MessageHandlerConfig(schema, handler, options))
    return this
  }

  build() {
    return this.configs
  }
}

export type Handler<MessagePayloadSchemas, ExecutionContext> = (
  message: MessagePayloadSchemas,
  context: ExecutionContext,
) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<MessagePayloadSchemas extends object, ExecutionContext> = {
  messageHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]
  messageTypeField: string
}

export class HandlerContainer<MessagePayloadSchemas extends object, ExecutionContext> {
  private readonly messageHandlers: Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>
  >
  private readonly messageTypeField: string

  constructor(options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext>) {
    this.messageTypeField = options.messageTypeField
    this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public resolveHandler(
    messageType: string,
  ): MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext> {
    const handler = this.messageHandlers[messageType]
    if (!handler) {
      throw new Error(`Unsupported message type: ${messageType}`)
    }
    return handler
  }

  private resolveHandlerMap(
    supportedHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[],
  ): Record<string, MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>> {
    return supportedHandlers.reduce(
      (acc, entry) => {
        // @ts-ignore
        const messageType = entry.schema.shape[this.messageTypeField].value
        acc[messageType] = entry
        return acc
      },
      {} as Record<string, MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>>,
    )
  }
}
