import z, {
  type ZodLiteral,
  type ZodObject,
  type ZodOptional,
  type ZodString,
  type ZodRawShape,
} from 'zod'

import {
  CONSUMER_BASE_EVENT_SCHEMA,
  GENERATED_BASE_EVENT_SCHEMA,
  OPTIONAL_GENERATED_BASE_EVENT_SCHEMA,
  PUBLISHER_BASE_EVENT_SCHEMA,
} from '../events/baseEventSchemas'
import type { CommonEventDefinition } from '../events/eventTypes'

// External message metadata that describe the context in which the message was created, primarily used for debugging purposes
export const PUBLISHER_MESSAGE_METADATA_SCHEMA = z
  .object({
    schemaVersion: z.optional(z.string().min(1).describe('message schema version')),
    // this is always set to a service that created the message
    producedBy: z.optional(z.string().min(1).describe('app/service that produced the message')),
    // this is always propagated within the message chain. For the first message in the chain it is equal to "producedBy"
    originatedFrom: z.optional(
      z
        .string()
        .min(1)
        .describe('app/service that initiated entire workflow that led to creating this message'),
    ),
    // this is always propagated within the message chain.
    correlationId: z.optional(
      z.string().describe('unique identifier passed to all events in workflow chain'),
    ),
  })
  .describe('external message metadata')

export const CONSUMER_MESSAGE_METADATA_SCHEMA = z
  .object({
    schemaVersion: z.string().min(1).describe('message schema version'),
    // this is always set to a service that created the message
    producedBy: z.string().min(1).describe('app/service that produced the message'),
    // this is always propagated within the message chain. For the first message in the chain it is equal to "producedBy"
    originatedFrom: z
      .string()
      .min(1)
      .describe('app/service that initiated entire workflow that led to creating this message'),
    // this is always propagated within the message chain.
    correlationId: z.string().describe('unique identifier passed to all events in workflow chain'),
  })
  .describe('external message metadata')

export const CONSUMER_MESSAGE_SCHEMA_EXTENSION = {
  // For internal domain events that did not originate within a message chain metadata field can be omitted, producer should then assume it is initiating a new chain
  metadata: CONSUMER_MESSAGE_METADATA_SCHEMA,
}

export const PUBLISHER_MESSAGE_SCHEMA_EXTENSION = {
  // For internal domain events that did not originate within a message chain metadata field can be omitted, producer should then assume it is initiating a new chain
  metadata: PUBLISHER_MESSAGE_METADATA_SCHEMA.optional(),
}

export const CONSUMER_BASE_MESSAGE_SCHEMA = CONSUMER_BASE_EVENT_SCHEMA.extend(
  CONSUMER_MESSAGE_SCHEMA_EXTENSION,
)
export const PUBLISHER_BASE_MESSAGE_SCHEMA = PUBLISHER_BASE_EVENT_SCHEMA.extend(
  PUBLISHER_MESSAGE_SCHEMA_EXTENSION,
)

export type ConsumerBaseMessageType = z.infer<typeof CONSUMER_BASE_MESSAGE_SCHEMA>
export type PublisherBaseMessageType = z.infer<typeof PUBLISHER_BASE_MESSAGE_SCHEMA>

export type PublisherMessageMetadataType = z.infer<typeof PUBLISHER_MESSAGE_METADATA_SCHEMA>
export type ConsumerMessageMetadataType = z.infer<typeof CONSUMER_MESSAGE_METADATA_SCHEMA>

export type CommonMessageDefinitionSchemaType<T extends CommonEventDefinition> = z.infer<
  T['consumerSchema']
>

type ReturnType<T extends ZodObject<Y>, Y extends ZodRawShape, Z extends string> = {
  consumerSchema: ZodObject<{
    id: ZodString
    timestamp: ZodString
    type: ZodLiteral<Z>
    payload: T
    metadata: ZodObject<{
      schemaVersion: ZodString
      producedBy: ZodString
      originatedFrom: ZodString
      correlationId: ZodString
    }>
  }>

  publisherSchema: ZodObject<{
    id: ZodOptional<ZodString>
    timestamp: ZodOptional<ZodString>
    type: ZodLiteral<Z>
    payload: T
    metadata: ZodOptional<
      ZodObject<{
        schemaVersion: ZodOptional<ZodString>
        producedBy: ZodOptional<ZodString>
        originatedFrom: ZodOptional<ZodString>
        correlationId: ZodOptional<ZodString>
      }>
    >
  }>
}

export type SchemaMetadata = {
  description: string
}

export function enrichMessageSchemaWithBaseStrict<
  T extends ZodObject<Y>,
  Y extends ZodRawShape,
  Z extends string,
>(type: Z, payloadSchema: T, schemaMetadata: SchemaMetadata): ReturnType<T, Y, Z> {
  return enrichMessageSchemaWithBase(type, payloadSchema, schemaMetadata)
}

export function enrichMessageSchemaWithBase<
  T extends ZodObject<Y>,
  Y extends ZodRawShape,
  Z extends string,
>(type: Z, payloadSchema: T, schemaMetadata?: Partial<SchemaMetadata>): ReturnType<T, Y, Z> {
  const baseSchema = z.object({
    type: z.literal(type),
    payload: payloadSchema,
  })

  let consumerSchema = GENERATED_BASE_EVENT_SCHEMA.merge(baseSchema).extend(
    CONSUMER_MESSAGE_SCHEMA_EXTENSION,
  )
  let publisherSchema = OPTIONAL_GENERATED_BASE_EVENT_SCHEMA.merge(baseSchema).extend(
    PUBLISHER_MESSAGE_SCHEMA_EXTENSION,
  )

  if (schemaMetadata?.description) {
    consumerSchema = consumerSchema.describe(schemaMetadata.description)
    publisherSchema = publisherSchema.describe(schemaMetadata.description)
  }

  return {
    consumerSchema: consumerSchema,
    publisherSchema: publisherSchema,
  }
}

export function getMessageType<T extends ZodObject<Y>, Y extends ZodRawShape, Z extends string>(
  richMessageSchema: ReturnType<T, Y, Z>,
): Z {
  return richMessageSchema.consumerSchema.shape.type.value
}
