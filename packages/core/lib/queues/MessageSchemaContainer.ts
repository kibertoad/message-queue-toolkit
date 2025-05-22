import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v3'

export type MessageSchemaContainerOptions<MessagePayloadSchemas extends object> = {
  messageDefinitions: readonly CommonEventDefinition[]
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  messageTypeField: string
}

export class MessageSchemaContainer<MessagePayloadSchemas extends object> {
  public readonly messageDefinitions: Record<string, CommonEventDefinition>
  private readonly messageSchemas: Record<string, ZodSchema<MessagePayloadSchemas>>
  private readonly messageTypeField: string

  constructor(options: MessageSchemaContainerOptions<MessagePayloadSchemas>) {
    this.messageTypeField = options.messageTypeField
    this.messageSchemas = this.resolveSchemaMap(options.messageSchemas)
    this.messageDefinitions = this.resolveDefinitionsMap(options.messageDefinitions ?? [])
  }

  public resolveSchema(
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    message: Record<string, any>,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>> {
    const schema = this.messageSchemas[message[this.messageTypeField]]
    if (!schema) {
      return {
        error: new Error(`Unsupported message type: ${message[this.messageTypeField]}`),
      }
    }
    return {
      result: schema,
    }
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

  private resolveDefinitionsMap(
    supportedDefinitions: readonly CommonEventDefinition[],
  ): Record<string, CommonEventDefinition> {
    return supportedDefinitions.reduce(
      (acc, definition) => {
        // @ts-ignore
        acc[definition.publisherSchema.shape[this.messageTypeField].value] = definition
        return acc
      },
      {} as Record<string, CommonEventDefinition>,
    )
  }
}
