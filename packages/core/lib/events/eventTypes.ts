import type { ZodObject, ZodTypeAny } from 'zod'
import type z from 'zod'

import type { BASE_EVENT_SCHEMA } from './baseEventSchemas'

export type EventTypeNames<EventDefinition extends CommonEventDefinition> =
  CommonEventDefinitionSchemaType<EventDefinition>['type']

export type CommonEventDefinition = {
  schema: ZodObject<Omit<(typeof BASE_EVENT_SCHEMA)['shape'], 'payload'> & { payload: ZodTypeAny }>
}

export type CommonEventDefinitionSchemaType<T extends CommonEventDefinition> = z.infer<T['schema']>

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
