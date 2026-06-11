import { expectTypeOf } from 'vitest'
import { z } from 'zod/v4'
import { enrichMessageSchemaWithBase } from '../messages/baseMessageSchemas.ts'
import type {
  AnyEventHandler,
  CommonEventDefinition,
  CommonEventDefinitionConsumerSchemaType,
  CommonEventDefinitionPublisherSchemaType,
  SingleEventHandler,
} from './eventTypes.ts'

const myEvents = {
  plainEvent: {
    ...enrichMessageSchemaWithBase('entity.created', z.object({ name: z.string() })),
  },
  transformingEvent: {
    ...enrichMessageSchemaWithBase(
      'entity.updated',
      z.object({
        // Forward-compatible field: unknown values are dropped instead of failing validation
        mode: z.preprocess(
          (value) => (value === 'live' ? value : undefined),
          z.literal('live').optional(),
        ),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

describe('eventTypes', () => {
  describe('CommonEventDefinitionConsumerSchemaType', () => {
    it('resolves transformed fields to their output type, not their input type', () => {
      type ConsumerMessage = CommonEventDefinitionConsumerSchemaType<
        typeof myEvents.transformingEvent
      >

      // DomainEventEmitter hands handlers the event parsed by the schema
      expectTypeOf<ConsumerMessage['payload']['mode']>().toEqualTypeOf<'live' | undefined>()
    })
  })

  describe('CommonEventDefinitionPublisherSchemaType', () => {
    it('keeps the input type for transformed fields', () => {
      type PublisherMessage = CommonEventDefinitionPublisherSchemaType<
        typeof myEvents.transformingEvent
      >

      // Publishers pass the raw payload that the schema parses on emit
      expectTypeOf<PublisherMessage['payload']['mode']>().toBeUnknown()
    })
  })

  describe('SingleEventHandler', () => {
    it('receives the parsed event', () => {
      type Handler = SingleEventHandler<[typeof myEvents.transformingEvent], 'entity.updated'>
      type HandledEvent = Parameters<Handler['handleEvent']>[0]

      expectTypeOf<HandledEvent['payload']['mode']>().toEqualTypeOf<'live' | undefined>()
    })
  })

  describe('AnyEventHandler', () => {
    it('receives the parsed event union', () => {
      type Handler = AnyEventHandler<
        [typeof myEvents.plainEvent, typeof myEvents.transformingEvent]
      >
      type HandledEvent = Parameters<Handler['handleEvent']>[0]

      expectTypeOf<HandledEvent['type']>().toEqualTypeOf<'entity.created' | 'entity.updated'>()
      expectTypeOf<
        Extract<HandledEvent, { type: 'entity.updated' }>['payload']['mode']
      >().toEqualTypeOf<'live' | undefined>()
    })
  })
})
