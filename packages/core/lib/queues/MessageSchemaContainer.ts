import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v4'

export type MessageSchemaContainerOptions<MessagePayloadSchemas extends object> = {
  messageDefinitions: readonly CommonEventDefinition[]
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  messageTypeField?: string
}

const DEFAULT_SCHEMA_KEY = Symbol('NO_MESSAGE_TYPE')

export class MessageSchemaContainer<MessagePayloadSchemas extends object> {
  public readonly messageDefinitions: Record<string | symbol, CommonEventDefinition>
  // biome-ignore  lint/correctness/noUnusedPrivateClassMembers: this is actually used
  private readonly messageSchemas: Record<string | symbol, ZodSchema<MessagePayloadSchemas>>
  private readonly messageTypeField?: string

  constructor(options: MessageSchemaContainerOptions<MessagePayloadSchemas>) {
    this.messageTypeField = options.messageTypeField
    this.messageSchemas = this.resolveMap(options.messageSchemas)
    this.messageDefinitions = this.resolveMap(options.messageDefinitions ?? [])
  }

  public resolveSchema(
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    message: Record<string, any>,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>> {
    const messageType = this.messageTypeField ? message[this.messageTypeField] : undefined

    const schema = this.messageSchemas[messageType ?? DEFAULT_SCHEMA_KEY]
    if (!schema) {
      return {
        error: new Error(
          `Unsupported message type: ${messageType ?? DEFAULT_SCHEMA_KEY.toString()}`,
        ),
      }
    }
    return { result: schema }
  }

  // biome-ignore  lint/correctness/noUnusedPrivateClassMembers: this is actually used
  private resolveMap<T extends CommonEventDefinition | ZodSchema<MessagePayloadSchemas>>(
    array: readonly T[],
  ): Record<string | symbol, T> {
    const result: Record<string | symbol, T> = {}

    for (const item of array) {
      let type: string | undefined

      if (this.messageTypeField) {
        type =
          'publisherSchema' in item
            ? // @ts-expect-error
              item.publisherSchema?.shape[this.messageTypeField]?.value
            : // @ts-expect-error
              item.shape?.[this.messageTypeField]?.value
      }

      const key = type ?? DEFAULT_SCHEMA_KEY
      if (result[key]) throw new Error(`Duplicate schema for type: ${key.toString()}`)

      result[key] = item
    }

    return result
  }
}
