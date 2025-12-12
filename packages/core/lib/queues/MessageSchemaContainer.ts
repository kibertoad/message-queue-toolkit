import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v4'
import {
  extractMessageTypeFromSchema,
  isMessageTypeLiteralConfig,
  isMessageTypePathConfig,
  isMessageTypeResolverFnConfig,
  type MessageTypeResolverConfig,
  type MessageTypeResolverContext,
  resolveMessageType,
} from './MessageTypeResolver.ts'

export type MessageSchemaContainerOptions<MessagePayloadSchemas extends object> = {
  messageDefinitions: readonly CommonEventDefinition[]
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  /**
   * Configuration for resolving message types.
   */
  messageTypeResolver?: MessageTypeResolverConfig
}

const DEFAULT_SCHEMA_KEY = Symbol('NO_MESSAGE_TYPE')

export class MessageSchemaContainer<MessagePayloadSchemas extends object> {
  public readonly messageDefinitions: Record<string | symbol, CommonEventDefinition>

  private readonly messageSchemas: Record<string | symbol, ZodSchema<MessagePayloadSchemas>>
  private readonly messageTypeResolver?: MessageTypeResolverConfig

  constructor(options: MessageSchemaContainerOptions<MessagePayloadSchemas>) {
    this.messageTypeResolver = options.messageTypeResolver
    this.messageSchemas = this.resolveMap(options.messageSchemas)
    this.messageDefinitions = this.resolveMap(options.messageDefinitions ?? [])
  }

  /**
   * Resolves the schema for a message based on its type.
   *
   * @param message - The parsed message data
   * @param attributes - Optional message-level attributes (e.g., PubSub attributes)
   * @returns Either an error or the resolved schema
   */
  public resolveSchema(
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    message: Record<string, any>,
    attributes?: Record<string, unknown>,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>> {
    let messageType: string | undefined
    try {
      messageType = this.resolveMessageTypeFromData(message, attributes)
    } catch (e) {
      return { error: e instanceof Error ? e : new Error(String(e)) }
    }

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

  /**
   * Resolves message type from message data and optional attributes.
   */
  private resolveMessageTypeFromData(
    messageData: unknown,
    messageAttributes?: Record<string, unknown>,
  ): string | undefined {
    if (this.messageTypeResolver) {
      const context: MessageTypeResolverContext = { messageData, messageAttributes }
      return resolveMessageType(this.messageTypeResolver, context)
    }

    return undefined
  }

  /**
   * Gets the field path used for extracting message type from schemas during registration.
   * Returns undefined for literal or custom resolver modes.
   */
  private getMessageTypePathForSchema(): string | undefined {
    if (this.messageTypeResolver && isMessageTypePathConfig(this.messageTypeResolver)) {
      return this.messageTypeResolver.messageTypePath
    }
    // For literal or custom resolver, we don't extract type from schema
    return undefined
  }

  /**
   * Gets the literal message type if configured.
   */
  private getLiteralMessageType(): string | undefined {
    if (this.messageTypeResolver && isMessageTypeLiteralConfig(this.messageTypeResolver)) {
      return this.messageTypeResolver.literal
    }
    return undefined
  }

  private resolveMap<T extends CommonEventDefinition | ZodSchema<MessagePayloadSchemas>>(
    array: readonly T[],
  ): Record<string | symbol, T> {
    const result: Record<string | symbol, T> = {}

    const literalType = this.getLiteralMessageType()
    const messageTypePath = this.getMessageTypePathForSchema()

    // Validate: custom resolver function cannot be used with multiple schemas.
    // The resolver works fine for runtime type resolution (when messages arrive),
    // but at registration time we need to map each schema to its message type.
    // With a custom resolver, we can't know what types it will return until runtime,
    // so we can't build the type→schema lookup map for multiple schemas.
    if (
      this.messageTypeResolver &&
      isMessageTypeResolverFnConfig(this.messageTypeResolver) &&
      array.length > 1
    ) {
      throw new Error(
        'Custom resolver function cannot be used with multiple schemas. ' +
          'The resolver works for runtime type resolution, but at registration time ' +
          'we cannot determine which schema corresponds to which type. ' +
          'Use messageTypePath config (to extract types from schema literals) or register only a single schema.',
      )
    }

    for (const item of array) {
      let type: string | undefined

      // If literal type is configured, use it for all schemas
      if (literalType) {
        type = literalType
      } else if (messageTypePath) {
        // Extract type from schema shape using the field path
        type =
          'publisherSchema' in item
            ? extractMessageTypeFromSchema(item.publisherSchema, messageTypePath)
            : // @ts-expect-error - ZodSchema has shape property at runtime
              extractMessageTypeFromSchema(item, messageTypePath)
      }
      // For custom resolver without field path, we can't extract from schema
      // All schemas will use DEFAULT_SCHEMA_KEY

      const key = type ?? DEFAULT_SCHEMA_KEY
      if (result[key]) throw new Error(`Duplicate schema for type: ${key.toString()}`)

      result[key] = item
    }

    return result
  }
}
