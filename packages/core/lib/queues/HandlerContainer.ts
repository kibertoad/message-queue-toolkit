import type { Either } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { BarrierCallback } from './AbstractQueueService'

export class MessageHandlerConfig<
  const MessagePayloadSchemas extends object,
  const ExecutionContext,
> {
  public readonly schema: ZodSchema<MessagePayloadSchemas>
  public readonly handler: Handler<MessagePayloadSchemas, ExecutionContext>
  public readonly barrier?: BarrierCallback<MessagePayloadSchemas>

  constructor(
    schema: ZodSchema<MessagePayloadSchemas>,
    handler: Handler<MessagePayloadSchemas, ExecutionContext>,
    barrier?: BarrierCallback<MessagePayloadSchemas>,
  ) {
    this.schema = schema
    this.handler = handler
    this.barrier = barrier
  }
}

export class MessageHandlerConfigBuilder<MessagePayloadSchemas extends object, ExecutionContext> {
  private readonly configs: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext>[]

  constructor() {
    this.configs = []
  }

  addConfig<MessagePayloadSchema extends MessagePayloadSchemas>(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext>,
    barrier?: BarrierCallback<MessagePayloadSchema>,
  ) {
    // @ts-ignore
    this.configs.push(new MessageHandlerConfig(schema, handler, barrier))
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
