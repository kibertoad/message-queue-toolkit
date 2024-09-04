import {
  type SnsAwareEventDefinition,
  enrichEventSchemaWithBase,
} from '@message-queue-toolkit/schemas'
import { describe } from 'vitest'
import { z } from 'zod'
import type { OutboxStorage } from './outbox'

const TEST_EVENT_SCHEMA = z.object({
  name: z.string(),
  age: z.number(),
})

const testEvents = {
  'test-event.something-happened': {
    ...enrichEventSchemaWithBase('test-event.something-happened', TEST_EVENT_SCHEMA),
    snsTopic: 'TEST_TOPIC',
    producedBy: ['unit tes'],
  },
} satisfies Record<string, SnsAwareEventDefinition>

console.log(testEvents)

const testEventsArray = [...Object.values(testEvents)] satisfies SnsAwareEventDefinition[]

type TestEvents = typeof testEventsArray

class InMemoryOutbox implements OutboxStorage<TestEvents> {}

describe('outbox', () => {
  it('saves outbox entry to process it later', async () => {})
})
