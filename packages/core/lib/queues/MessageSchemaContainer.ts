import type { ZodSchema } from 'zod'

export type MessageSchemaContainerOptions<MessagePayloadSchemas extends object> = {
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  messageTypeField: string
}

export class MessageSchemaContainer<MessagePayloadSchemas extends object> {
  private readonly messageSchemas: Record<string, ZodSchema<MessagePayloadSchemas>>
  private readonly messageTypeField: string

  constructor(options: MessageSchemaContainerOptions<MessagePayloadSchemas>) {
    this.messageSchemas = this.resolveSchemaMap(options.messageSchemas)
    this.messageTypeField = options.messageTypeField
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public resolveSchema(message: Record<string, any>): ZodSchema<MessagePayloadSchemas> {
    const schema = this.messageSchemas[message[this.messageTypeField]]
    if (!schema) {
      throw new Error(`Unsupported message type: ${message[this.messageTypeField]}`)
    }
    return schema
  }

  private resolveSchemaMap(
    supportedSchemas: readonly ZodSchema<MessagePayloadSchemas>[],
  ): Record<string, ZodSchema<MessagePayloadSchemas>> {
    return supportedSchemas.reduce(
      (acc, schema) => {
        // @ts-ignore
        acc[schema.shape[this.messageTypeField].value] = schema
        return acc
      },
      {} as Record<string, ZodSchema<MessagePayloadSchemas>>,
    )
  }
}
