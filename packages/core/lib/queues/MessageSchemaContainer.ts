import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v3'

export type MessageSchemaContainerOptions<MessagePayloadSchemas extends object> = {
  messageDefinitions: readonly CommonEventDefinition[]
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  messageTypeField?: string
}

const NO_MESSAGE_TYPE = 'NO_MESSAGE_TYPE'

export class MessageSchemaContainer<MessagePayloadSchemas extends object> {
  public readonly messageDefinitions: Record<string, CommonEventDefinition>
  private readonly messageSchemas: Record<string, ZodSchema<MessagePayloadSchemas>>
  private readonly messageTypeField?: string

  constructor(options: MessageSchemaContainerOptions<MessagePayloadSchemas>) {
    if (options.messageTypeField === undefined && options.messageSchemas.length > 1) {
      throw new Error(
        'if messageTypeField is not provided, messageSchemas must have a single schema',
      )
    }

    this.messageTypeField = options.messageTypeField
    this.messageSchemas = this.resolveSchemaMap(options.messageSchemas)
    this.messageDefinitions = this.resolveDefinitionsMap(options.messageDefinitions ?? [])
  }

  public resolveSchema(
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    message: Record<string, any>,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>> {
    if (!this.messageTypeField) {
      return this.messageSchemas[NO_MESSAGE_TYPE]
        ? { result: this.messageSchemas[NO_MESSAGE_TYPE] }
        : { error: new Error('Unsupported message') }
    }

    const schema = this.messageSchemas[message[this.messageTypeField]]
    if (!schema) {
      return { error: new Error(`Unsupported message type: ${message[this.messageTypeField]}`) }
    }
    return { result: schema }
  }

  private resolveSchemaMap(
    supportedSchemas: readonly ZodSchema<MessagePayloadSchemas>[],
  ): Record<string, ZodSchema<MessagePayloadSchemas>> {
    if (!this.messageTypeField) {
      if (!supportedSchemas[0]) return {}
      return { [NO_MESSAGE_TYPE]: supportedSchemas[0] }
    }

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
    if (!this.messageTypeField) {
      if (!supportedDefinitions[0]) return {}
      return { [NO_MESSAGE_TYPE]: supportedDefinitions[0] }
    }

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
