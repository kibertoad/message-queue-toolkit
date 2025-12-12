/**
 * Context passed to a custom message type resolver function.
 * Contains both the parsed message data and any message-level attributes/metadata.
 */
export type MessageTypeResolverContext = {
  /**
   * The parsed/decoded message body (e.g., JSON-parsed data field in PubSub)
   */
  messageData: unknown
  /**
   * Message-level attributes/metadata (e.g., PubSub message attributes, SQS message attributes)
   * This is where Cloud Storage notifications put eventType, for example.
   */
  messageAttributes?: Record<string, unknown>
}

/**
 * Function that extracts the message type from a message.
 * Used for routing messages to appropriate handlers and schemas.
 *
 * The function MUST return a valid message type string. If the type cannot be
 * determined, the function should either:
 * - Return a default type (e.g., 'unknown' or a fallback handler type)
 * - Throw an error with a descriptive message
 *
 * @example
 * // Extract from attributes (e.g., Cloud Storage notifications)
 * const resolver: MessageTypeResolverFn = ({ messageAttributes }) => {
 *   const eventType = messageAttributes?.eventType as string | undefined
 *   if (!eventType) {
 *     throw new Error('eventType attribute is required')
 *   }
 *   return eventType
 * }
 *
 * @example
 * // Extract from nested data with fallback
 * const resolver: MessageTypeResolverFn = ({ messageData }) => {
 *   const data = messageData as { metadata?: { eventName?: string } }
 *   return data.metadata?.eventName ?? 'default.event'
 * }
 */
export type MessageTypeResolverFn = (context: MessageTypeResolverContext) => string

/**
 * Configuration for resolving message types.
 *
 * Three modes are supported:
 *
 * 1. **Field path** (string): Extract type from a field at the root of the message.
 *    @example { messageTypePath: 'type' } // extracts type from message.type
 *    @example { messageTypePath: 'detail-type' } // for EventBridge events
 *
 * 2. **Constant type** (object with `literal`): All messages are treated as the same type.
 *    Useful when a subscription/queue only receives one type of message.
 *    @example { literal: 'order.created' }
 *
 * 3. **Custom resolver** (object with `resolver`): Full flexibility via callback function.
 *    Use when type needs to be extracted from attributes, nested fields, or requires transformation.
 *    @example
 *    {
 *      resolver: ({ messageAttributes }) => messageAttributes?.eventType as string
 *    }
 *
 * @example
 * // Cloud Storage notifications via PubSub - type is in attributes
 * const config: MessageTypeResolverConfig = {
 *   resolver: ({ messageAttributes }) => {
 *     const eventType = messageAttributes?.eventType as string | undefined
 *     if (!eventType) {
 *       throw new Error('eventType attribute is required for Cloud Storage notifications')
 *     }
 *     // Optionally map to your internal event types
 *     if (eventType === 'OBJECT_FINALIZE') return 'storage.object.created'
 *     if (eventType === 'OBJECT_DELETE') return 'storage.object.deleted'
 *     return eventType
 *   }
 * }
 *
 * @example
 * // CloudEvents format - type might be in envelope or data
 * const config: MessageTypeResolverConfig = {
 *   resolver: ({ messageData, messageAttributes }) => {
 *     // Check for CloudEvents binary mode (type in attributes)
 *     if (messageAttributes?.['ce-type']) {
 *       return messageAttributes['ce-type'] as string
 *     }
 *     // Fall back to type in message data
 *     const data = messageData as { type?: string }
 *     if (!data.type) {
 *       throw new Error('Message type not found in CloudEvents envelope or message data')
 *     }
 *     return data.type
 *   }
 * }
 */
export type MessageTypeResolverConfig =
  | {
      /**
       * Field name at the root of the message containing the message type.
       */
      messageTypePath: string
    }
  | {
      /**
       * Constant message type for all messages.
       * Use when all messages in a queue/subscription are of the same type.
       */
      literal: string
    }
  | {
      /**
       * Custom function to extract message type from message data and/or attributes.
       * Provides full flexibility for complex routing scenarios.
       */
      resolver: MessageTypeResolverFn
    }

/**
 * Type guard to check if config uses field path mode
 */
export function isMessageTypePathConfig(
  config: MessageTypeResolverConfig,
): config is { messageTypePath: string } {
  return 'messageTypePath' in config
}

/**
 * Type guard to check if config uses literal/constant mode
 */
export function isMessageTypeLiteralConfig(
  config: MessageTypeResolverConfig,
): config is { literal: string } {
  return 'literal' in config
}

/**
 * Type guard to check if config uses custom resolver mode
 */
export function isMessageTypeResolverFnConfig(
  config: MessageTypeResolverConfig,
): config is { resolver: MessageTypeResolverFn } {
  return 'resolver' in config
}

/**
 * Resolves message type using the provided configuration.
 *
 * @param config - The resolver configuration
 * @param context - Context containing message data and attributes
 * @returns The resolved message type
 * @throws Error if message type cannot be resolved (for messageTypePath mode)
 */
export function resolveMessageType(
  config: MessageTypeResolverConfig,
  context: MessageTypeResolverContext,
): string {
  if (isMessageTypeLiteralConfig(config)) {
    return config.literal
  }

  if (isMessageTypePathConfig(config)) {
    const data = context.messageData as Record<string, unknown> | undefined
    const messageType = data?.[config.messageTypePath] as string | undefined
    if (messageType === undefined) {
      throw new Error(
        `Unable to resolve message type: field '${config.messageTypePath}' not found in message data`,
      )
    }
    return messageType
  }

  // Custom resolver function - must return a string (user handles errors/defaults)
  return config.resolver(context)
}

/**
 * Extracts message type from schema definition using the field path.
 * Used during handler/schema registration to build the routing map.
 *
 * @param schema - Zod schema with shape property
 * @param messageTypePath - Field name containing the type literal
 * @returns The literal type value from the schema, or undefined if field doesn't exist or isn't a literal
 */
export function extractMessageTypeFromSchema(
  // biome-ignore lint/suspicious/noExplicitAny: Schema shape can be any
  schema: { shape?: Record<string, any> },
  messageTypePath: string | undefined,
): string | undefined {
  if (!messageTypePath) {
    return undefined
  }

  const field = schema.shape?.[messageTypePath]
  if (!field) {
    return undefined
  }

  // Check if the field has a literal value (z.literal() creates a field with .value)
  if (!('value' in field)) {
    return undefined
  }

  return field.value as string | undefined
}
