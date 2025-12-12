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
    // If no resolver configured, use the single default schema
    if (!this.messageTypeResolver) {
      const schema = this.messageSchemas[DEFAULT_SCHEMA_KEY]
      if (!schema) {
        return {
          error: new Error('No messageTypeResolver configured and no default schema available'),
        }
      }
      return { result: schema }
    }

    let messageType: string
    try {
      messageType = this.resolveMessageTypeFromData(message, attributes)
    } catch (e) {
      return { error: e instanceof Error ? e : new Error(String(e)) }
    }

    const schema = this.messageSchemas[messageType]
    if (!schema) {
      return {
        error: new Error(`Unsupported message type: ${messageType}`),
      }
    }
    return { result: schema }
  }

  /**
   * Resolves message type from message data and optional attributes.
   * Only called when messageTypeResolver is configured.
   */
  private resolveMessageTypeFromData(
    messageData: unknown,
    messageAttributes?: Record<string, unknown>,
  ): string {
    // This method is only called after checking messageTypeResolver exists in resolveSchema
    const resolver = this.messageTypeResolver as MessageTypeResolverConfig
    const context: MessageTypeResolverContext = { messageData, messageAttributes }
    return resolveMessageType(resolver, context)
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

  /**
   * Validates that multiple schemas can be properly mapped at registration time.
   */
  private validateMultipleSchemas(schemaCount: number): void {
    if (schemaCount <= 1) return

    if (!this.messageTypeResolver) {
      throw new Error(
        'Multiple schemas require messageTypeResolver to be configured. ' +
          'Use messageTypePath config (to extract types from schema literals) or literal config.',
      )
    }
    // Custom resolver function cannot be used with multiple schemas because
    // we can't know what types it will return until runtime.
    if (isMessageTypeResolverFnConfig(this.messageTypeResolver)) {
      throw new Error(
        'Custom resolver function cannot be used with multiple schemas. ' +
          'The resolver works for runtime type resolution, but at registration time ' +
          'we cannot determine which schema corresponds to which type. ' +
          'Use messageTypePath config (to extract types from schema literals) or register only a single schema.',
      )
    }
  }

  private resolveMap<T extends CommonEventDefinition | ZodSchema<MessagePayloadSchemas>>(
    array: readonly T[],
  ): Record<string | symbol, T> {
    const result: Record<string | symbol, T> = {}

    this.validateMultipleSchemas(array.length)

    const literalType = this.getLiteralMessageType()
    const messageTypePath = this.getMessageTypePathForSchema()

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
      // For single schema without resolver, use DEFAULT_SCHEMA_KEY

      const key = type ?? DEFAULT_SCHEMA_KEY
      if (result[key]) throw new Error(`Duplicate schema for type: ${key.toString()}`)

      result[key] = item
    }

    return result
  }
}
