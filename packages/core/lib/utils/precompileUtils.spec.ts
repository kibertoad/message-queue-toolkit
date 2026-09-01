import { describe, expect, it } from 'vitest'
import { type ZodType, z } from 'zod/v4'
import { EventRegistry } from '../events/EventRegistry.ts'
import type { CommonEventDefinition } from '../events/eventTypes.ts'
import { MessageHandlerConfigBuilder } from '../queues/HandlerContainer.ts'
import { MessageSchemaContainer } from '../queues/MessageSchemaContainer.ts'
import {
  isPrecompiledSchema,
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
    expect(isPrecompiledSchema(MESSAGE_SCHEMA)).toBe(false)
    expect(isPrecompiledSchema(precompiled)).toBe(true)
  })

  it('parses exactly like the schema it was built from', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(precompiled.parse(VALID_MESSAGE)).toEqual(MESSAGE_SCHEMA.parse(VALID_MESSAGE))
    expect(() => precompiled.parse({ type: 'message.a', payload: { name: 42 } })).toThrow(
      z.ZodError,
    )
  })

  it('is idempotent', () => {
    const precompiled = precompileSchema(MESSAGE_SCHEMA)

    expect(precompileSchema(precompiled)).toBe(precompiled)
  })

  it('hands back schemas that zod refuses to compile, still usable', async () => {
    const asyncSchema = z.string().refine(async (value) => value.length > 0)

    const precompiled = precompileSchema(asyncSchema)

    expect(precompiled).toBe(asyncSchema)
    await expect(precompiled.parseAsync('a')).resolves.toBe('a')
  })

  it('does not expose the marker as an enumerable property', () => {
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
    expect(isPrecompiledSchema(precompiled.consumerSchema)).toBe(true)
    expect(isPrecompiledSchema(precompiled.publisherSchema)).toBe(true)
    expect(isPrecompiledSchema(definition.consumerSchema)).toBe(false)
    expect(precompiled.schemaVersion).toBe('1.0.0')
  })
})

describe('automatic precompilation', () => {
  it('precompiles schemas registered on a MessageSchemaContainer', () => {
    const container = new MessageSchemaContainer<z.infer<typeof MESSAGE_SCHEMA>>({
      messageSchemas: [{ schema: MESSAGE_SCHEMA }],
      messageDefinitions: [],
    })

    const resolved = container.resolveSchema(VALID_MESSAGE)

    expect('result' in resolved).toBe(true)
    if ('result' in resolved && resolved.result) {
      expect(isPrecompiledSchema(resolved.result)).toBe(true)
      expect(resolved.result.parse(VALID_MESSAGE)).toEqual(VALID_MESSAGE)
    }
  })

  it('precompiles schemas registered on a message handler', () => {
    const configs = new MessageHandlerConfigBuilder<z.infer<typeof MESSAGE_SCHEMA>, undefined>()
      .addConfig(MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' as const }))
      .build()

    expect(isPrecompiledSchema(configs[0]?.schema as ZodType)).toBe(true)
  })

  it('precompiles the definitions an EventRegistry resolves', () => {
    const eventDefinition = {
      consumerSchema: MESSAGE_SCHEMA,
      publisherSchema: MESSAGE_SCHEMA,
    } as unknown as CommonEventDefinition
    const registry = new EventRegistry([eventDefinition])

    const resolved = registry.getEventDefinitionByTypeName('message.a')

    expect(isPrecompiledSchema(resolved.publisherSchema)).toBe(true)
    // The array the caller passed in is left as it was
    expect(registry.supportedEvents[0]).toBe(eventDefinition)
  })
})
