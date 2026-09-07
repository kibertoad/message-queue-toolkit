import { describe, expectTypeOf, it } from 'vitest'
import { z } from 'zod/v4'
import type { EventPayloadSchema, RejectAnyPayload } from './baseEventSchemas.ts'

const objectSchema = z.object({ a: z.string() })
const unionSchema = z.union([z.object({ a: z.string() }), z.object({ b: z.number() })])

describe('EventPayloadSchema', () => {
  it('accepts an object payload schema', () => {
    expectTypeOf<typeof objectSchema>().toExtend<EventPayloadSchema>()
  })

  it('accepts a union-of-objects payload schema', () => {
    expectTypeOf<typeof unionSchema>().toExtend<EventPayloadSchema>()
  })

  it('does not accept a bare scalar payload schema', () => {
    expectTypeOf<z.ZodString>().not.toExtend<EventPayloadSchema>()
  })
})

describe('RejectAnyPayload', () => {
  it('collapses an `any`-output payload schema to never', () => {
    expectTypeOf<RejectAnyPayload<z.ZodAny>>().toBeNever()
  })

  it('leaves a real object payload schema untouched', () => {
    expectTypeOf<RejectAnyPayload<typeof objectSchema>>().not.toBeNever()
  })
})
