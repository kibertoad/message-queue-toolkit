import type { CommonLogger } from '@lokalise/node-core'
import type { Message } from '@platformatic/kafka'
import type { ZodSchema } from 'zod/v4'

export interface RequestContext {
  logger: CommonLogger
  reqId: string
}

export type KafkaHandler<MessageValue extends object, ExecutionContext> = (
  message: Message<string, MessageValue, string, string>,
  context: ExecutionContext,
  requestContext: RequestContext,
) => Promise<void> | void

export class KafkaHandlerConfig<MessageValue extends object, ExecutionContext> {
  // biome-ignore lint/suspicious/noExplicitAny: Input for schema is flexible
  public readonly schema: ZodSchema<MessageValue, any>
  public readonly handler: KafkaHandler<MessageValue, ExecutionContext>

  constructor(
    // biome-ignore lint/suspicious/noExplicitAny: Input for schema is flexible
    schema: ZodSchema<MessageValue, any>,
    handler: KafkaHandler<MessageValue, ExecutionContext>,
  ) {
    this.schema = schema
    this.handler = handler
  }
}
