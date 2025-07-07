import z, {
  type ZodLiteral,
  type ZodObject,
  type ZodOptional,
  type ZodString,
  type ZodRawShape,
  type ZodNullable,
  type ZodISODateTime,
} from 'zod/v4'
import {
  CONSUMER_BASE_EVENT_SCHEMA,
  type CONSUMER_MESSAGE_METADATA_SCHEMA,
  GENERATED_BASE_EVENT_SCHEMA,
  OPTIONAL_GENERATED_BASE_EVENT_SCHEMA,
  PUBLISHER_BASE_EVENT_SCHEMA,
  type PUBLISHER_MESSAGE_METADATA_SCHEMA,
} from '../events/baseEventSchemas.ts'
import type { CommonEventDefinition } from '../events/eventTypes.ts'
import type { MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA } from './messageDeduplicationSchemas.ts'

export const CONSUMER_BASE_MESSAGE_SCHEMA = CONSUMER_BASE_EVENT_SCHEMA
export const PUBLISHER_BASE_MESSAGE_SCHEMA = PUBLISHER_BASE_EVENT_SCHEMA

export type ConsumerBaseMessageType = z.input<typeof CONSUMER_BASE_MESSAGE_SCHEMA>
export type PublisherBaseMessageType = z.input<typeof PUBLISHER_BASE_MESSAGE_SCHEMA>

export type PublisherMessageMetadataType = z.input<typeof PUBLISHER_MESSAGE_METADATA_SCHEMA>
export type ConsumerMessageMetadataType = z.input<typeof CONSUMER_MESSAGE_METADATA_SCHEMA>

export type CommonMessageDefinitionSchemaType<T extends CommonEventDefinition> = z.input<
  T['consumerSchema']
>

// IDE type inference works better for whatever reason if MetadataObject is directly shared between ReturnType and CommonEventDefinition
export type MetadataObject = ZodObject<{
  schemaVersion: ZodString
  producedBy: ZodString
  originatedFrom: ZodString
  correlationId: ZodString
}>

export const MetadataObjectSchema = z.object({
  schemaVersion: z.string(),
  producedBy: z.string(),
  originatedFrom: z.string(),
  correlationId: z.string(),
})

type ReturnType<T extends ZodObject<Y>, Y extends ZodRawShape, Z extends string> = {
  consumerSchema: ZodObject<{
    id: ZodString
    timestamp: ZodISODateTime
    type: ZodLiteral<Z>
    deduplicationId: ZodOptional<ZodNullable<ZodString>>
    deduplicationOptions: ZodOptional<ZodNullable<typeof MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA>>
    payload: T
    metadata: MetadataObject
  }>

  publisherSchema: ZodObject<{
    id: ZodOptional<ZodString>
    timestamp: ZodOptional<ZodISODateTime>
    type: ZodLiteral<Z>
    deduplicationId: ZodOptional<ZodNullable<ZodString>>
    deduplicationOptions: ZodOptional<ZodNullable<typeof MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA>>
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

  let consumerSchema = GENERATED_BASE_EVENT_SCHEMA.merge(baseSchema)
  let publisherSchema = OPTIONAL_GENERATED_BASE_EVENT_SCHEMA.merge(baseSchema)

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
