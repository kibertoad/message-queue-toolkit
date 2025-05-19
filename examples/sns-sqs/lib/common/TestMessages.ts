import {
  type AllConsumerMessageSchemas,
  type SnsAwareEventDefinition,
  enrichMessageSchemaWithBaseStrict,
} from '@message-queue-toolkit/schemas'
import { z } from 'zod'

export const USER_SCHEMA = z.object({
  id: z.string(),
  name: z.string(),
  age: z.number().optional(),
})

export const UserEvents = {
  created: {
    ...enrichMessageSchemaWithBaseStrict('user.created', USER_SCHEMA, {
      description: 'User was created',
    }),
    schemaVersion: '1.0.1',
    producedBy: ['user-service'],
    domain: 'users',
    snsTopic: 'user',
  },

  updated: {
    ...enrichMessageSchemaWithBaseStrict('user.updated', USER_SCHEMA, {
      description: 'User was updated',
    }),
    schemaVersion: '1.0.1',
    producedBy: ['user-service'],
    domain: 'users',
    snsTopic: 'user',
  },
} satisfies Record<string, SnsAwareEventDefinition>

export type UserEventsType = (typeof UserEvents)[keyof typeof UserEvents][]
export type UserEventConsumerPayloadsType = AllConsumerMessageSchemas<UserEventsType>
