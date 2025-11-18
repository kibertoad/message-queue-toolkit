import type { Either } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import { isCommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema } from 'zod/v4'

import type { DoNotProcessMessageError } from '../errors/DoNotProcessError.ts'
import type { RetryMessageLaterError } from '../errors/RetryMessageLaterError.ts'

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
  public readonly envelopeSchema?: ZodSchema<unknown>
  public readonly definition?: CommonEventDefinition
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
    envelopeSchema?: ZodSchema<unknown>,
  ) {
    this.schema = schema
    this.envelopeSchema = envelopeSchema
    this.definition = eventDefinition
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
   * Two-parameter version: Use when the message type field for routing can be resolved from the payload schema.
   * The same schema is used for both routing (to match the message type) and validation (for the handler).
   *
   * Use this when:
   * - The entire message is the payload (messagePayloadField is undefined)
   * - The message type field (e.g., 'type') is at the root level of the message
   * - Handler receives and processes the entire message
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
  ): this

  /**
   * Three-parameter version: Use when the message type field for routing is only available in the message envelope,
   * but the handler only needs to process a nested payload field.
   *
   * Use this when:
   * - Payload extraction is configured (messagePayloadField is set, e.g., 'detail')
   * - The message type field is in the envelope/root (e.g., 'detail-type')
   * - Handler should only receive the extracted payload, not the full envelope
   *
   * The envelope schema is used for routing (must have a literal type field), and the payload schema
   * is used for validating the extracted payload that the handler receives.
   *
   * Example (EventBridge events):
   * ```typescript
   * // Envelope schema with literal detail-type for routing
   * const USER_CREATED_ENVELOPE = z.object({
   *   'detail-type': z.literal('user.created'),
   *   time: z.string(),
   *   detail: z.object({ userId: z.string(), email: z.string() })
   * })
   *
   * // Payload schema for validation
   * const USER_CREATED_DETAIL = z.object({
   *   userId: z.string(),
   *   email: z.string()
   * })
   *
   * builder.addConfig(
   *   USER_CREATED_ENVELOPE,  // Used for routing by 'detail-type'
   *   USER_CREATED_DETAIL,    // Used for validating extracted 'detail'
   *   async (detail) => {
   *     // detail only has userId and email, not the envelope fields
   *   }
   * )
   * ```
   */
  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    envelopeSchema: ZodSchema<unknown>,
    payloadSchema: ZodSchema<MessagePayloadSchema>,
    handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    options?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
  ): this

  // Implementation
  addConfig<MessagePayloadSchema extends MessagePayloadSchemas, const BarrierOutput>(
    schemaOrEnvelope: ZodSchema<MessagePayloadSchema> | CommonEventDefinition | ZodSchema<unknown>,
    handlerOrPayloadSchema:
      | Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>
      | ZodSchema<MessagePayloadSchema>,
    optionsOrHandler?:
      | HandlerConfigOptions<
          MessagePayloadSchema,
          ExecutionContext,
          PrehandlerOutput,
          BarrierOutput
        >
      | Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>,
    maybeOptions?: HandlerConfigOptions<
      MessagePayloadSchema,
      ExecutionContext,
      PrehandlerOutput,
      BarrierOutput
    >,
  ) {
    // Detect which overload was called based on parameter types
    const isThreeParamVersion =
      typeof handlerOrPayloadSchema === 'object' && 'parse' in handlerOrPayloadSchema

    let payloadSchema: ZodSchema<MessagePayloadSchema>
    let handler: Handler<MessagePayloadSchema, ExecutionContext, PrehandlerOutput, BarrierOutput>
    let options:
      | HandlerConfigOptions<
          MessagePayloadSchema,
          ExecutionContext,
          PrehandlerOutput,
          BarrierOutput
        >
      | undefined
    let definition: CommonEventDefinition | undefined
    let envelopeSchema: ZodSchema<unknown> | undefined

    if (isThreeParamVersion) {
      // Three-param version: (envelopeSchema, payloadSchema, handler, options?)
      envelopeSchema = schemaOrEnvelope as ZodSchema<unknown>
      payloadSchema = handlerOrPayloadSchema as ZodSchema<MessagePayloadSchema>
      handler = optionsOrHandler as Handler<
        MessagePayloadSchema,
        ExecutionContext,
        PrehandlerOutput,
        BarrierOutput
      >
      options = maybeOptions
      definition = undefined
    } else {
      // Two-param version: (schema, handler, options?) - schema used for both routing and validation
      const schema = schemaOrEnvelope as ZodSchema<MessagePayloadSchema> | CommonEventDefinition
      payloadSchema = isCommonEventDefinition(schema)
        ? // @ts-ignore
          (schema.consumerSchema as ZodSchema<MessagePayloadSchema>)
        : schema
      definition = isCommonEventDefinition(schema) ? schema : undefined
      handler = handlerOrPayloadSchema as Handler<
        MessagePayloadSchema,
        ExecutionContext,
        PrehandlerOutput,
        BarrierOutput
      >
      options = optionsOrHandler as
        | HandlerConfigOptions<
            MessagePayloadSchema,
            ExecutionContext,
            PrehandlerOutput,
            BarrierOutput
          >
        | undefined
      // If envelopeSchema not provided, routing will use payloadSchema
      envelopeSchema = undefined
    }

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
        envelopeSchema,
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
  messageTypeField: string
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
  private readonly messageTypeField: string

  constructor(
    options: HandlerContainerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
  ) {
    this.messageTypeField = options.messageTypeField
    this.messageHandlers = this.resolveHandlerMap(options.messageHandlers)
  }

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
    return supportedHandlers.reduce(
      (acc, entry) => {
        // Use envelopeSchema for routing if provided, otherwise use payloadSchema
        const schemaForRouting = entry.envelopeSchema ?? entry.schema
        // @ts-expect-error
        const messageType = schemaForRouting.shape[this.messageTypeField].value
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
