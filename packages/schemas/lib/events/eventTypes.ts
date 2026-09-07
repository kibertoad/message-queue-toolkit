import { z } from 'zod/v4'

import { MetadataObjectSchema } from '../messages/baseMessageSchemas.ts'
import { CONSUMER_BASE_EVENT_SCHEMA, PUBLISHER_BASE_EVENT_SCHEMA } from './baseEventSchemas.ts'

/**
 * Schema allowed as an event payload: anything whose output is an object, which covers a
 * plain object schema and a union of object variants (e.g. a single-item / multi-item shape).
 * It stays object-based (a bare string/number payload is not allowed) while accepting unions.
 * Defined via the output type (not `ZodObject | ZodUnion`) so `.extend` does not distribute
 * over a type-level union and collapse it back to a plain object.
 */
export type EventPayloadSchema = z.ZodType<Record<string, unknown>>

export type EventTypeNames<EventDefinition extends CommonEventDefinition> =
  CommonEventDefinitionConsumerSchemaType<EventDefinition>['type']

export function isCommonEventDefinition(entity: unknown): entity is CommonEventDefinition {
  return (entity as CommonEventDefinition).publisherSchema !== undefined
}

const consumerSchema = CONSUMER_BASE_EVENT_SCHEMA.extend({
  metadata: MetadataObjectSchema,
  payload: z.looseObject({}) as EventPayloadSchema,
})

const publisherSchema = PUBLISHER_BASE_EVENT_SCHEMA.extend({
  payload: z.looseObject({}) as EventPayloadSchema,
})

export type CommonEventDefinition = {
  consumerSchema: typeof consumerSchema
  publisherSchema: typeof publisherSchema
  schemaVersion?: string

  //
  // Metadata used for automated documentation generation
  //
  producedBy?: readonly string[] // Service ids for all the producers of this event.
  domain?: string // Domain of the event
  tags?: readonly string[] // Free-form tags for the event
}

// Consumers receive messages already parsed by the consumer schema, hence the output type.
export type CommonEventDefinitionConsumerSchemaType<T extends CommonEventDefinition> = z.output<
  T['consumerSchema']
>

export type CommonEventDefinitionPublisherSchemaType<T extends CommonEventDefinition> = z.input<
  T['publisherSchema']
>

export type EventHandler<
  EventDefinitionSchema extends
    CommonEventDefinitionConsumerSchemaType<CommonEventDefinition> = CommonEventDefinitionConsumerSchemaType<CommonEventDefinition>,
> = {
  readonly eventHandlerId: string
  handleEvent(event: EventDefinitionSchema): void | Promise<void>
}

export type AnyEventHandler<EventDefinitions extends CommonEventDefinition[]> = EventHandler<
  CommonEventDefinitionConsumerSchemaType<EventDefinitions[number]>
>

export type SingleEventHandler<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = EventHandler<EventFromArrayByTypeName<EventDefinition, EventTypeName>>

type EventFromArrayByTypeName<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = Extract<
  CommonEventDefinitionConsumerSchemaType<EventDefinition[number]>,
  { type: EventTypeName }
>
