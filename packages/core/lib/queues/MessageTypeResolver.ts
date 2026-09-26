import { getProperty } from 'dot-prop'

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
 * 1. **Field path** (string): Extract type from a field in the message using dot notation.
 *    Supports nested paths like 'metadata.type' or 'detail.eventType'.
 *    @example { messageTypePath: 'type' } // extracts from message.type
 *    @example { messageTypePath: 'detail-type' } // for EventBridge events
 *    @example { messageTypePath: 'metadata.eventType' } // nested path
 *
 * 2. **Constant type** (object with `literal`): All messages are treated as the same type.
 *    Useful when a subscription/queue only receives one type of message.
 *    @example { literal: 'order.created' }
 *
 * 3. **Custom resolver** (object with `resolver`): Full flexibility via callback function.
 *    Use when type needs to be extracted from attributes or requires transformation.
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
       * Path to the field containing the message type.
       * Supports dot notation for nested paths (e.g., 'metadata.type', 'detail.eventType').
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
    const rawMessageType = getProperty(context.messageData, config.messageTypePath)
    if (rawMessageType === undefined || rawMessageType === null) {
      throw new Error(
        `Unable to resolve message type: path '${config.messageTypePath}' not found in message data`,
      )
    }
    if (typeof rawMessageType !== 'string') {
      throw new Error(
        `Unable to resolve message type: path '${config.messageTypePath}' contains a non-string value (got ${typeof rawMessageType})`,
      )
    }
    return rawMessageType
  }

  // Custom resolver function - must return a string (user handles errors/defaults)
  return config.resolver(context)
}

type JsonSchemaNode = {
  $ref?: string
  $defs?: Record<string, JsonSchemaNode>
  definitions?: Record<string, JsonSchemaNode>
  properties?: Record<string, JsonSchemaNode>
  const?: unknown
}

/**
 * The part of the Standard JSON Schema interface (`~standard.jsonSchema`) used to read literal
 * field values from a schema. zod 4.2+ schemas implement it.
 */
export type StandardJsonSchemaSource = {
  readonly '~standard': {
    readonly vendor: string
    readonly jsonSchema?: {
      readonly input: (options: {
        readonly target: string
        readonly libraryOptions?: Record<string, unknown>
      }) => Record<string, unknown>
    }
  }
}

/**
 * Per-vendor `libraryOptions` that make the JSON Schema converter emit `{}` for a field it cannot
 * represent (such as a `Date` or a custom check) instead of throwing. Only the message type field
 * is read, so the other fields' types do not matter.
 */
const LENIENT_LIBRARY_OPTIONS: Record<string, Record<string, unknown>> = {
  zod: { unrepresentable: 'any' },
  arktype: { fallback: (ctx: { base: unknown }) => ctx.base },
  valibot: { errorMode: 'ignore' },
}

const inputJsonSchemaCache = new WeakMap<object, JsonSchemaNode | undefined>()

function getInputJsonSchema(schema: unknown): JsonSchemaNode | undefined {
  if (typeof schema !== 'object' || schema === null) {
    return undefined
  }
  if (inputJsonSchemaCache.has(schema)) {
    return inputJsonSchemaCache.get(schema)
  }

  const props = (schema as Partial<StandardJsonSchemaSource>)['~standard']
  const jsonSchema = props?.jsonSchema?.input({
    target: 'draft-2020-12',
    libraryOptions: LENIENT_LIBRARY_OPTIONS[props.vendor],
  }) as JsonSchemaNode | undefined
  inputJsonSchemaCache.set(schema, jsonSchema)
  return jsonSchema
}

function resolveRef(root: JsonSchemaNode, node: JsonSchemaNode): JsonSchemaNode | undefined {
  if (!node.$ref) {
    return node
  }
  const match = /^#\/(\$defs|definitions)\/(.+)$/.exec(node.$ref)
  if (!match) {
    return undefined
  }
  const defs = match[1] === '$defs' ? root.$defs : root.definitions
  return defs?.[match[2] as string]
}

/**
 * Extracts message type from schema definition using the field path.
 * Used during handler/schema registration to build the routing map.
 * Supports dot notation for nested paths (e.g., 'metadata.type').
 *
 * The value is read from the schema's Standard JSON Schema input representation
 * (`~standard.jsonSchema`): the field at the path must have a string `const`, which is how a literal
 * is represented. The converted JSON Schema is cached per schema object.
 *
 * @param schema - Schema implementing Standard JSON Schema
 * @param messageTypePath - Path to the field containing the type literal (supports dot notation)
 * @returns The literal type value from the schema, or undefined if field doesn't exist or isn't a literal
 */
export function extractMessageTypeFromSchema(
  schema: unknown,
  messageTypePath: string | undefined,
): string | undefined {
  if (!messageTypePath) {
    return undefined
  }

  const root = getInputJsonSchema(schema)
  if (!root) {
    return undefined
  }

  let current: JsonSchemaNode | undefined = root
  for (const part of messageTypePath.split('.')) {
    current = current && resolveRef(root, current)
    current = current?.properties?.[part]
    if (!current) {
      return undefined
    }
  }

  const value = resolveRef(root, current)?.const
  return typeof value === 'string' ? value : undefined
}
