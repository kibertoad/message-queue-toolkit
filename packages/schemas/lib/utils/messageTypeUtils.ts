import type { z } from 'zod'

import type { CommonEventDefinition } from '../events/eventTypes'

/**
 * Resolves schema of a consumer message for a given event definition
 */
export type ConsumerMessageSchema<MessageDefinitionType extends CommonEventDefinition> = z.infer<
  MessageDefinitionType['consumerSchema']
>

/**
 * Resolves schema of a publishder message for a given event definition
 */
export type PublisherMessageSchema<MessageDefinitionType extends CommonEventDefinition> = z.infer<
  MessageDefinitionType['publisherSchema']
>
