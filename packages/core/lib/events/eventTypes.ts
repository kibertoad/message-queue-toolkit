import type { ZodObject, ZodTypeAny } from 'zod'
import type z from 'zod'

import type { MessageMetadataType } from '../messages/baseMessageSchemas'

import type { CONSUMER_BASE_EVENT_SCHEMA, PUBLISHER_BASE_EVENT_SCHEMA } from './baseEventSchemas'

export type EventTypeNames<EventDefinition extends CommonEventDefinition> =
  CommonEventDefinitionConsumerSchemaType<EventDefinition>['type']

export type CommonEventDefinition = {
  consumerSchema: ZodObject<
    Omit<(typeof CONSUMER_BASE_EVENT_SCHEMA)['shape'], 'payload'> & { payload: ZodTypeAny }
  >
  publisherSchema: ZodObject<
    Omit<(typeof PUBLISHER_BASE_EVENT_SCHEMA)['shape'], 'payload'> & { payload: ZodTypeAny }
  >
  schemaVersion?: string
}

export type CommonEventDefinitionConsumerSchemaType<T extends CommonEventDefinition> = z.infer<
  T['consumerSchema']
>

export type CommonEventDefinitionPublisherSchemaType<T extends CommonEventDefinition> = z.infer<
  T['publisherSchema']
>

export type EventHandler<
  EventDefinitionSchema extends
    CommonEventDefinitionConsumerSchemaType<CommonEventDefinition> = CommonEventDefinitionConsumerSchemaType<CommonEventDefinition>,
  MetadataDefinitionSchema extends Partial<MessageMetadataType> = Partial<MessageMetadataType>,
> = {
  handleEvent(
    event: EventDefinitionSchema,
    metadata?: MetadataDefinitionSchema,
  ): void | Promise<void>
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
