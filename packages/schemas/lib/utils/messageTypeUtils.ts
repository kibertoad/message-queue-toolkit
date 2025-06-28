import type { z } from 'zod/v4'

import type { CommonEventDefinition } from '../events/eventTypes.ts'

/**
 * Resolves schema of a consumer message for a given event definition
 */
export type ConsumerMessageSchema<MessageDefinitionType extends CommonEventDefinition> = z.infer<
  MessageDefinitionType['consumerSchema']
>

/**
 * Resolves schema of a publisher message for a given event definition
 */
export type PublisherMessageSchema<MessageDefinitionType extends CommonEventDefinition> = z.infer<
  MessageDefinitionType['publisherSchema']
>

/**
 * Resolves schema of all possible publisher messages for a given list of event definitions
 */
export type AllPublisherMessageSchemas<MessageDefinitionTypes extends CommonEventDefinition[]> =
  z.infer<MessageDefinitionTypes[number]['publisherSchema']>

/**
 * Resolves schema of all possible consumer messages for a given list of event definitions
 */
export type AllConsumerMessageSchemas<MessageDefinitionTypes extends CommonEventDefinition[]> =
  z.infer<MessageDefinitionTypes[number]['consumerSchema']>
