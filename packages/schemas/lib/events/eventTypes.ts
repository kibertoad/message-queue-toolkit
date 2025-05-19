import type { ZodObject, ZodTypeAny } from 'zod'
import type z from 'zod'

import type { MetadataObject } from '../messages/baseMessageSchemas.js'
import type { CONSUMER_BASE_EVENT_SCHEMA, PUBLISHER_BASE_EVENT_SCHEMA } from './baseEventSchemas.ts'

export type EventTypeNames<EventDefinition extends CommonEventDefinition> =
  CommonEventDefinitionConsumerSchemaType<EventDefinition>['type']

export function isCommonEventDefinition(entity: unknown): entity is CommonEventDefinition {
  return (entity as CommonEventDefinition).publisherSchema !== undefined
}

export type CommonEventDefinition = {
  consumerSchema: ZodObject<
    Omit<(typeof CONSUMER_BASE_EVENT_SCHEMA)['shape'], 'payload' | 'metadata'> & {
      metadata: MetadataObject
      payload: ZodTypeAny
    }
  >
  publisherSchema: ZodObject<
    Omit<(typeof PUBLISHER_BASE_EVENT_SCHEMA)['shape'], 'payload'> & {
      payload: ZodTypeAny
    }
  >
  schemaVersion?: string

  //
  // Metadata used for automated documentation generation
  //
  producedBy?: readonly string[] // Service ids for all the producers of this event.
  domain?: string // Domain of the event
  tags?: readonly string[] // Free-form tags for the event
}

export type CommonEventDefinitionConsumerSchemaType<T extends CommonEventDefinition> = z.infer<
  T['consumerSchema']
>

export type CommonEventDefinitionPublisherSchemaType<T extends CommonEventDefinition> = z.infer<
  T['publisherSchema']
>

export type EventHandler<
  EventDefinitionSchema extends
    CommonEventDefinitionPublisherSchemaType<CommonEventDefinition> = CommonEventDefinitionPublisherSchemaType<CommonEventDefinition>,
> = {
  readonly eventHandlerId: string
  handleEvent(event: EventDefinitionSchema): void | Promise<void>
}

export type AnyEventHandler<EventDefinitions extends CommonEventDefinition[]> = EventHandler<
  CommonEventDefinitionPublisherSchemaType<EventDefinitions[number]>
>

export type SingleEventHandler<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = EventHandler<EventFromArrayByTypeName<EventDefinition, EventTypeName>>

type EventFromArrayByTypeName<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = Extract<
  CommonEventDefinitionPublisherSchemaType<EventDefinition[number]>,
  { type: EventTypeName }
>
