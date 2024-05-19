import type { ZodObject, ZodTypeAny } from 'zod'
import type z from 'zod'

import type { CONSUMER_BASE_EVENT_SCHEMA, PUBLISHER_BASE_EVENT_SCHEMA } from './baseEventSchemas'

export type EventTypeNames<EventDefinition extends CommonEventDefinition> =
  CommonEventDefinitionSchemaType<EventDefinition>['type']

export type CommonEventDefinition = {
  consumerSchema: ZodObject<
    Omit<(typeof CONSUMER_BASE_EVENT_SCHEMA)['shape'], 'payload'> & { payload: ZodTypeAny }
  >
  publisherSchema: ZodObject<
    Omit<(typeof PUBLISHER_BASE_EVENT_SCHEMA)['shape'], 'payload'> & { payload: ZodTypeAny }
  >
  schemaVersion?: string
}

export type CommonEventDefinitionSchemaType<T extends CommonEventDefinition> = z.infer<
  T['consumerSchema']
>

export type EventHandler<
  EventDefinitionSchema extends
    CommonEventDefinitionSchemaType<CommonEventDefinition> = CommonEventDefinitionSchemaType<CommonEventDefinition>,
> = {
  handleEvent(event: EventDefinitionSchema): void | Promise<void>
}

export type AnyEventHandler<EventDefinitions extends CommonEventDefinition[]> = EventHandler<
  CommonEventDefinitionSchemaType<EventDefinitions[number]>
>

export type SingleEventHandler<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = EventHandler<EventFromArrayByTypeName<EventDefinition, EventTypeName>>

type EventFromArrayByTypeName<
  EventDefinition extends CommonEventDefinition[],
  EventTypeName extends EventTypeNames<EventDefinition[number]>,
> = Extract<CommonEventDefinitionSchemaType<EventDefinition[number]>, { type: EventTypeName }>
