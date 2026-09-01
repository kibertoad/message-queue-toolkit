import { precompileSchema } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v4'
import type { DeserializedMessage, RequestContext } from '../types.ts'

export type KafkaHandler<
  MessageValue extends object,
  ExecutionContext,
  BatchProcessingEnabled extends boolean = false,
> = (
  message: BatchProcessingEnabled extends false
    ? DeserializedMessage<MessageValue>
    : DeserializedMessage<MessageValue>[],
  context: ExecutionContext,
  requestContext: RequestContext,
) => Promise<void> | void

export class KafkaHandlerConfig<
  MessageValue extends object,
  ExecutionContext,
  BatchProcessingEnabled extends boolean = false,
> {
  // biome-ignore lint/suspicious/noExplicitAny: Input for schema is flexible
  public readonly schema: ZodSchema<MessageValue, any>
  public readonly handler: KafkaHandler<MessageValue, ExecutionContext, BatchProcessingEnabled>

  constructor(
    // biome-ignore lint/suspicious/noExplicitAny: Input for schema is flexible
    schema: ZodSchema<MessageValue, any>,
    handler: KafkaHandler<MessageValue, ExecutionContext, BatchProcessingEnabled>,
  ) {
    // Every message routed to this handler is parsed with the schema, so it is worth compiling
    this.schema = precompileSchema(schema)
    this.handler = handler
  }
}

export class KafkaBatchHandlerConfig<
  MessageValue extends object,
  ExecutionContext,
> extends KafkaHandlerConfig<MessageValue, ExecutionContext, true> {}
