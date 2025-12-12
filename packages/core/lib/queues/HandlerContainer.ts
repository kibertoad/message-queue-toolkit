import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import { isCommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v4'

import type { DoNotProcessMessageError } from '../errors/DoNotProcessError.ts'
import type { RetryMessageLaterError } from '../errors/RetryMessageLaterError.ts'
import {
  extractMessageTypeFromSchema,
  isMessageTypeLiteralConfig,
  isMessageTypePathConfig,
  type MessageTypeResolverConfig,
  type MessageTypeResolverContext,
  resolveMessageType,
} from './MessageTypeResolver.ts'

export type PreHandlingOutputs<PrehandlerOutput = undefined, BarrierOutput = undefined> = {
  preHandlerOutput: PrehandlerOutput
  barrierOutput: BarrierOutput
}

export type LogFormatter<MessagePayloadSchema> = (message: MessagePayloadSchema) => unknown

export type BarrierResult<BarrierOutput> =
  | BarrierResultPositive<BarrierOutput>
  | BarrierResultNegative

export type BarrierResultPositive<BarrierOutput> = {
  isPassing: true
  output: BarrierOutput
}

export type BarrierResultNegative = {
  isPassing: false
  output?: never
}

export type PrehandlerResult = Either<DoNotProcessMessageError | RetryMessageLaterError, 'success'>

export type BarrierCallback<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput,
> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  preHandlerOutput: PrehandlerOutput,
) => Promise<BarrierResult<BarrierOutput>>

export type Prehandler<MessagePayloadSchema extends object, ExecutionContext, PrehandlerOutput> = (
  message: MessagePayloadSchema,
  context: ExecutionContext,
  preHandlerOutput: Partial<PrehandlerOutput>,
  next: (result: PrehandlerResult) => void,
) => void

export const defaultLogFormatter = <MessagePayloadSchema>(message: MessagePayloadSchema) => message

export type HandlerConfigOptions<
  MessagePayloadSchema extends object,
  ExecutionContext,
  PrehandlerOutput,
  BarrierOutput,
> = {
  /**
   * Explicit message type for this handler.
   * Required when using a custom resolver function that cannot extract types from schemas.
   * Optional when using messageTypePath or literal resolver modes.
   */
  messageType?: string
  messageLogFormatter?: LogFormatter<MessagePayloadSchema>
  preHandlerBarrier?: BarrierCallback<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  preHandlers?: Prehandler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput>[]
}

export class MessageHandlerConfig<
  const MessagePayloadSchema extends object,
  const ExecutionContext,
  const PrehandlerOutput = undefined,
  const BarrierOutput = unknown,
> {
  public readonly schema: ZodSchema<MessagePayloadSchema>
  public readonly definition?: CommonEventDefinition
  /**
   * Explicit message type for this handler, if provided.
   * Used for routing when type cannot be extracted from schema.
   */
  public readonly messageType?: string
  public readonly handler: Handler<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly messageLogFormatter: LogFormatter<MessagePayloadSchema>
  public readonly preHandlerBarrier?: BarrierCallback<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  public readonly preHandlers: Prehandler<
    MessagePayloadSchema,
    ExecutionContext,
    PrehandlerOutput
  >[]

  constructor(
    schema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
    eventDefinition?: CommonEventDefinition,
  ) {
    this.schema = schema
    this.definition = eventDefinition
    this.messageType = options?.messageType
    this.handler = handler
    this.messageLogFormatter = options?.messageLogFormatter ?? defaultLogFormatter
    this.preHandlerBarrier = options?.preHandlerBarrier
    this.preHandlers = options?.preHandlers ?? []
  }
}

export class MessageHandlerConfigBuilder<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> {
  private readonly configs: MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    any
  >[]

  constructor() {
    this.configs = []
  }

  /**
   * Add a handler configuration for a specific message type.
   * The schema is used for both routing (to match the message type) and validation (for the handler).
   *
   * The message type field (e.g., 'type' or 'detail-type') must be at the root level of the message
   * and must be a literal value in the schema for routing to work.
   *
   * Example:
   * ```typescript
   * const USER_CREATED_SCHEMA = z.object({
   *   type: z.literal('user.created'),
   *   userId: z.string(),
   *   email: z.string()
   * })
   *
   * builder.addConfig(USER_CREATED_SCHEMA, async (message) => {
   *   // message has type 'user.created', userId, and email
   * })
   * ```
   *
   * EventBridge example:
   * ```typescript
   * const USER_PRESENCE_SCHEMA = z.object({
   *   'detail-type': z.literal('v2.users.{id}.presence'),
   *   time: z.string(),
   *   detail: z.object({
   *     userId: z.string(),
   *     presenceStatus: z.string()
   *   })
   * })
   *
   * builder.addConfig(USER_PRESENCE_SCHEMA, async (message) => {
   *   // message is the full EventBridge envelope
   *   const detail = message.detail  // Access nested payload directly
   * })
   * ```
   */
  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    schema: ZodSchema<MessagePayloadSchema> | CommonEventDefinition,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
  ): this {
    const payloadSchema = isCommonEventDefinition(schema)
      ? // @ts-ignore
        (schema.consumerSchema as ZodSchema<MessagePayloadSchema>)
      : schema
    const definition = isCommonEventDefinition(schema) ? schema : undefined

    this.configs.push(
      new MessageHandlerConfig<
        MessagePayloadSchemas,
        ExecutionContext,
        PrehandlerOutput,
        BarrierOutput
      >(
        payloadSchema,
        // @ts-expect-error
        handler,
        options,
        definition,
      ),
    )
    return this
  }

  build() {
    return this.configs
  }
}

export type Handler<
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput = undefined,
  BarrierOutput = undefined,
> = (
  message: MessagePayloadSchemas,
  context: ExecutionContext,
  preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, BarrierOutput>,
  definition?: CommonEventDefinition,
) => Promise<Either<'retryLater', 'success'>>

export type HandlerContainerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> = {
  messageHandlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
  /**
   * Configuration for resolving message types.
   */
  messageTypeResolver?: MessageTypeResolverConfig
}

export class HandlerContainer<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> {
  private readonly messageHandlers: Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
  >
  private readonly messageTypeResolver?: MessageTypeResolverConfig

  constructor(
    options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
  ) {
    this.messageTypeResolver = options.messageTypeResolver
    this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
  }

  /**
   * Resolves a handler for the given message type.
   */
  public resolveHandler<PrehandlerOutput = undefined, BarrierOutput = undefined>(
    messageType: string,
  ): MessageHandlerConfig<
    MessagePayloadSchemas,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  > {
    const handler = this.messageHandlers[messageType]
    if (!handler) {
      throw new Error(`Unsupported message type: ${messageType}`)
    }
    // @ts-expect-error
    return handler
  }

  /**
   * Resolves message type from message data and optional attributes using the configured resolver.
   *
   * @param messageData - The parsed message data
   * @param messageAttributes - Optional message-level attributes (e.g., PubSub attributes)
   * @returns The resolved message type
   * @throws Error if message type cannot be resolved
   */
  public resolveMessageType(
    messageData: unknown,
    messageAttributes?: Record<string, unknown>,
  ): string {
    if (this.messageTypeResolver) {
      const context: MessageTypeResolverContext = { messageData, messageAttributes }
      return resolveMessageType(this.messageTypeResolver, context)
    }

    throw new Error('Unable to resolve message type: messageTypeResolver is not configured')
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

  private resolveHandlerMap(
    supportedHandlers: MessageHandlerConfig<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput
    >[],
  ): Record<
    string,
    MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
  > {
    const literalType = this.getLiteralMessageType()
    const messageTypePath = this.getMessageTypePathForSchema()

    return supportedHandlers.reduce(
      (acc, entry) => {
        let messageType: string | undefined

        // Priority 1: Explicit messageType on the handler config
        if (entry.messageType) {
          messageType = entry.messageType
        }
        // Priority 2: Literal type from resolver config (same for all handlers)
        else if (literalType) {
          messageType = literalType
        }
        // Priority 3: Extract type from schema shape using the field path
        else if (messageTypePath) {
          // @ts-expect-error - ZodSchema has shape property at runtime
          messageType = extractMessageTypeFromSchema(entry.schema, messageTypePath)
        }

        if (!messageType) {
          throw new Error(
            'Unable to determine message type for handler. ' +
              'Either provide messageType in handler options, use a literal resolver, ' +
              'or ensure the schema has a literal type field matching messageTypePath.',
          )
        }

        if (acc[messageType]) {
          throw new Error(`Duplicate handler for message type: ${messageType}`)
        }

        acc[messageType] = entry
        return acc
      },
      {} as Record<
        string,
        MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
      >,
    )
  }
}
