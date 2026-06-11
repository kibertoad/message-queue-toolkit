import { expectTypeOf } from 'vitest'
import { z } from 'zod/v4'
import type {
  CommonEventDefinition,
  CommonEventDefinitionConsumerSchemaType,
} from '../events/eventTypes.ts'
import { enrichMessageSchemaWithBase } from '../messages/baseMessageSchemas.ts'
import type {
  AllConsumerMessageSchemas,
  ConsumerMessageSchema,
  PublisherMessageSchema,
} from './messageTypeUtils.ts'

const SUPPORTED_MODES = ['status', 'value'] as const

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
          (value) =>
            typeof value === 'string' && (SUPPORTED_MODES as readonly string[]).includes(value)
              ? value
              : undefined,
          z.enum(SUPPORTED_MODES).optional(),
        ),
      }),
    ),
  },
} as const satisfies Record<string, CommonEventDefinition>

describe('messageTypeUtils', () => {
  describe('ConsumerMessageSchema', () => {
    it('resolves to the parsed message type for transform-free schemas', () => {
      type ConsumerMessage = ConsumerMessageSchema<typeof myEvents.plainEvent>

      expectTypeOf<ConsumerMessage['type']>().toEqualTypeOf<'entity.created'>()
      expectTypeOf<ConsumerMessage['payload']['name']>().toEqualTypeOf<string>()
    })

    it('resolves transformed fields to their output type, not their input type', () => {
      type ConsumerMessage = ConsumerMessageSchema<typeof myEvents.transformingEvent>

      // Consumers receive messages already parsed by the consumer schema, so the
      // field is the preprocess output, not unknown (the input of any preprocess)
      expectTypeOf<ConsumerMessage['payload']['mode']>().toEqualTypeOf<
        'status' | 'value' | undefined
      >()
    })
  })

  describe('PublisherMessageSchema', () => {
    it('resolves to the raw (pre-parse) message type', () => {
      type PublisherMessage = PublisherMessageSchema<typeof myEvents.plainEvent>

      expectTypeOf<PublisherMessage['payload']['name']>().toEqualTypeOf<string>()
    })

    it('keeps the input type for transformed fields', () => {
      type PublisherMessage = PublisherMessageSchema<typeof myEvents.transformingEvent>

      // Publishers pass the raw payload that the schema parses on emit
      expectTypeOf<PublisherMessage['payload']['mode']>().toBeUnknown()
    })
  })

  describe('CommonEventDefinitionConsumerSchemaType', () => {
    it('resolves transformed fields to their output type, like ConsumerMessageSchema', () => {
      type ConsumerMessage = CommonEventDefinitionConsumerSchemaType<
        typeof myEvents.transformingEvent
      >

      // DomainEventEmitter hands handlers the event parsed by the schema
      expectTypeOf<ConsumerMessage['payload']['mode']>().toEqualTypeOf<
        'status' | 'value' | undefined
      >()
    })
  })

  describe('AllConsumerMessageSchemas', () => {
    it('resolves to the union of parsed message types', () => {
      type SupportedMessages = AllConsumerMessageSchemas<
        [typeof myEvents.plainEvent, typeof myEvents.transformingEvent]
      >

      expectTypeOf<SupportedMessages['type']>().toEqualTypeOf<'entity.created' | 'entity.updated'>()
    })
  })
})
