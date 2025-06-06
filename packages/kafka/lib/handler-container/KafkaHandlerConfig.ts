import type { CommonLogger } from '@lokalise/node-core'
import type { Message } from '@platformatic/kafka'
import type { ZodSchema } from 'zod'

export interface RequestContext {
  logger: CommonLogger
  reqId: string
}

export type KafkaHandler<MessageValue extends object> = (
  message: Message<string, MessageValue, string, string>,
  requestContext: RequestContext,
) => Promise<void> | void

export class KafkaHandlerConfig<MessageValue extends object> {
  public readonly schema: ZodSchema<MessageValue>
  public readonly handler: KafkaHandler<MessageValue>

  constructor(schema: ZodSchema<MessageValue>, handler: KafkaHandler<MessageValue>) {
    this.schema = schema
    this.handler = handler
  }
}
