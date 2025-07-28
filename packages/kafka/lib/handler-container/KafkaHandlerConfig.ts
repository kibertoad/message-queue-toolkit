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
  public readonly schema: ZodSchema<MessageValue, ZodTypeDef, unknown>
  public readonly handler: KafkaHandler<MessageValue, ExecutionContext>

  constructor(
    schema: ZodSchema<MessageValue, ZodTypeDef, unknown>,
    handler: KafkaHandler<MessageValue, ExecutionContext>,
  ) {
    this.schema = schema
    this.handler = handler
  }
}
