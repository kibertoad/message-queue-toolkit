import z, {
  type ZodLiteral,
  type ZodObject,
  type ZodOptional,
  type ZodString,
  type ZodRawShape,
} from 'zod'

import {
    CONSUMER_BASE_EVENT_SCHEMA, CONSUMER_MESSAGE_METADATA_SCHEMA,
    GENERATED_BASE_EVENT_SCHEMA,
    OPTIONAL_GENERATED_BASE_EVENT_SCHEMA,
    PUBLISHER_BASE_EVENT_SCHEMA, PUBLISHER_MESSAGE_METADATA_SCHEMA,
} from '../events/baseEventSchemas'
import type { CommonEventDefinition } from '../events/eventTypes'

export const CONSUMER_BASE_MESSAGE_SCHEMA = CONSUMER_BASE_EVENT_SCHEMA
export const PUBLISHER_BASE_MESSAGE_SCHEMA = PUBLISHER_BASE_EVENT_SCHEMA

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
