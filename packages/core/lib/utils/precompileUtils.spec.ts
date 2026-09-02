import { describe, expect, it } from 'vitest'
import { z } from 'zod/v4'
import { EventRegistry } from '../events/EventRegistry.ts'
import type { CommonEventDefinition } from '../events/eventTypes.ts'
import { MessageHandlerConfigBuilder } from '../queues/HandlerContainer.ts'
import { MessageSchemaContainer } from '../queues/MessageSchemaContainer.ts'
import {
  excludeFromPrecompilation,
  precompileEventDefinition,
  precompileSchema,
} from './precompileUtils.ts'

const MESSAGE_SCHEMA = z.object({
  type: z.literal('message.a'),
  payload: z.object({ name: z.string() }),
})

const VALID_MESSAGE = { type: 'message.a', payload: { name: 'test' } } as const

describe('precompileSchema', () => {
  it('leaves the original schema alone and returns a precompiled clone', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(precompiled).not.toBe(MESSAGE_SCHEMA)
  })

  it('parses exactly like the schema it was built from', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(precompiled.parse(VALID_MESSAGE)).toEqual(MESSAGE_SCHEMA.parse(VALID_MESSAGE))
    expect(() => precompiled.parse({ type: 'message.a', payload: { name: 42 } })).toThrow(
      z.ZodError,
    )
  })

  it('compiles a given schema once', () => {
    const schema = z.object({ payload: z.string() })

    expect(precompileSchema(schema)).toBe(precompileSchema(schema))
  })

  it('returns an already precompiled schema as is', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(precompileSchema(precompiled)).toBe(precompiled)
  })

  it('hands back schemas that zod refuses to compile, still usable', async () => {
    const asyncSchema = z.string().refine(async (value) => value.length > 0)

    const precompiled = precompileSchema(asyncSchema)

    expect(precompiled).toBe(asyncSchema)
    expect(precompileSchema(asyncSchema)).toBe(asyncSchema)
    await expect(precompiled.parseAsync('a')).resolves.toBe('a')
  })

  it('does not add enumerable properties to the schema', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(Object.keys(precompiled)).toEqual(Object.keys(MESSAGE_SCHEMA))
    expect(JSON.stringify(precompiled)).toEqual(JSON.stringify(MESSAGE_SCHEMA))
  })
})

describe('precompileEventDefinition', () => {
  const definition = {
    consumerSchema: MESSAGE_SCHEMA,
    publisherSchema: MESSAGE_SCHEMA,
    schemaVersion: '1.0.0',
  } as unknown as CommonEventDefinition

  it('precompiles both schemas without touching the definition it was given', () => {
    const precompiled = precompileEventDefinition(definition)

    expect(precompiled).not.toBe(definition)
    expect(precompiled.consumerSchema).toBe(precompileSchema(MESSAGE_SCHEMA))
    expect(precompiled.publisherSchema).toBe(precompileSchema(MESSAGE_SCHEMA))
    expect(definition.consumerSchema).toBe(MESSAGE_SCHEMA)
    expect(precompiled.schemaVersion).toBe('1.0.0')
  })

  it('precompiles a given definition once', () => {
    expect(precompileEventDefinition(definition)).toBe(precompileEventDefinition(definition))
    expect(precompileEventDefinition(precompileEventDefinition(definition))).toBe(
      precompileEventDefinition(definition),
    )
  })
})

describe('automatic precompilation', () => {
  it('precompiles schemas registered on a MessageSchemaContainer', () => {
    const container = new MessageSchemaContainer<z.infer<typeof MESSAGE_SCHEMA>>({
      messageSchemas: [{ schema: MESSAGE_SCHEMA }],
      messageDefinitions: [],
    })

    expect(container.resolveSchema(VALID_MESSAGE)).toEqual({
      result: precompileSchema(MESSAGE_SCHEMA),
    })
  })

  it('precompiles definitions registered on a MessageSchemaContainer', () => {
    const definition = {
      consumerSchema: MESSAGE_SCHEMA,
      publisherSchema: MESSAGE_SCHEMA,
    } as unknown as CommonEventDefinition
    const container = new MessageSchemaContainer<z.infer<typeof MESSAGE_SCHEMA>>({
      messageSchemas: [],
      messageDefinitions: [{ definition, messageType: 'message.a' }],
    })

    expect(container.messageDefinitions['message.a']).toBe(precompileEventDefinition(definition))
  })

  it('precompiles schemas registered on a message handler', () => {
    const configs = new MessageHandlerConfigBuilder<z.infer<typeof MESSAGE_SCHEMA>, undefined>()
      .addConfig(MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
      .build()

    expect(configs[0]?.schema).toBe(precompileSchema(MESSAGE_SCHEMA))
  })

  it('precompiles the definitions an EventRegistry resolves', () => {
    const eventDefinition = {
      consumerSchema: MESSAGE_SCHEMA,
      publisherSchema: MESSAGE_SCHEMA,
    } as unknown as CommonEventDefinition
    const registry = new EventRegistry([eventDefinition])

    const resolved = registry.getEventDefinitionByTypeName('message.a')

    expect(resolved.publisherSchema).toBe(precompileSchema(MESSAGE_SCHEMA))
    expect(resolved.consumerSchema).toBe(precompileSchema(MESSAGE_SCHEMA))
    // The array the caller passed in is left as it was
    expect(registry.supportedEvents[0]).toBe(eventDefinition)
  })
})

describe('excludeFromPrecompilation', () => {
  it('hands the schema back and keeps it out of compilation', () => {
    const schema = z.object({ payload: z.string() })

    expect(excludeFromPrecompilation(schema)).toBe(schema)
    expect(precompileSchema(schema)).toBe(schema)
  })

  it('holds even when the schema was already compiled', () => {
    const schema = z.object({ payload: z.string() })
    expect(precompileSchema(schema)).not.toBe(schema)

    excludeFromPrecompilation(schema)

    expect(precompileSchema(schema)).toBe(schema)
  })

  it('holds for a schema of an event definition', () => {
    const consumerSchema = excludeFromPrecompilation(z.object({ payload: z.string() }))
    const publisherSchema = z.object({ payload: z.string() })
    const definition = { consumerSchema, publisherSchema } as unknown as CommonEventDefinition

    const precompiled = precompileEventDefinition(definition)

    expect(precompiled.consumerSchema).toBe(consumerSchema)
    expect(precompiled.publisherSchema).not.toBe(publisherSchema)
  })

  it('holds for a schema registered on a message handler', () => {
    const schema = excludeFromPrecompilation(
      z.object({ type: z.literal('message.a'), payload: z.object({ name: z.string() }) }),
    )

    const configs = new MessageHandlerConfigBuilder<z.infer<typeof schema>, undefined>()
      .addConfig(schema, () => Promise.resolve({ result: 'success' as const }))
      .build()

    expect(configs[0]?.schema).toBe(schema)
  })
})

describe('callback invocation counts', () => {
  const REJECTED_MESSAGE = { type: 'message.a', payload: { name: 'rejected' } }

  const buildSchema = (onRefine: () => void) =>
    z
      .object({ type: z.literal('message.a'), payload: z.object({ name: z.string() }) })
      .refine((message) => {
        onRefine()
        return message.payload.name !== 'rejected'
      })

  it('runs a refinement once on a message that validates', () => {
    let calls = 0
    const precompiled = precompileSchema(
      buildSchema(() => {
        calls++
      }),
    )

    precompiled.parse(VALID_MESSAGE)

    expect(calls).toBe(1)
  })

  it('runs a refinement no more than twice on a message that fails validation', () => {
    // The compiled fast path signals rejection without building the error, so the interpreted
    // parser produces it and replays the refinement. Twice is the ceiling the README promises,
    // and the reason `excludeFromPrecompilation` exists.
    let calls = 0
    const precompiled = precompileSchema(
      buildSchema(() => {
        calls++
      }),
    )

    expect(() => precompiled.parse(REJECTED_MESSAGE)).toThrow(z.ZodError)

    expect(calls).toBeGreaterThanOrEqual(1)
    expect(calls).toBeLessThanOrEqual(2)
  })

  it('runs a refinement on an excluded schema once per parse, whether it passes or fails', () => {
    let calls = 0
    const schema = precompileSchema(
      excludeFromPrecompilation(
        buildSchema(() => {
          calls++
        }),
      ),
    )

    schema.parse(VALID_MESSAGE)
    expect(calls).toBe(1)

    calls = 0
    expect(() => schema.parse(REJECTED_MESSAGE)).toThrow(z.ZodError)
    expect(calls).toBe(1)
  })
})
