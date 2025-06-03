import type { ZodSchema } from 'zod'

export type KafkaHandler<MessageValue extends object> = (
  message: MessageValue,
) => Promise<void> | void

export class KafkaHandlerConfig<MessageValue extends object> {
  public readonly schema: ZodSchema<MessageValue>
  public readonly handler: KafkaHandler<MessageValue>

  constructor(schema: ZodSchema<MessageValue>, handler: KafkaHandler<MessageValue>) {
    this.schema = schema
    this.handler = handler
  }
}
