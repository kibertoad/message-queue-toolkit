import { compile, type ZodType } from 'zod/v4'
import type { CommonEventDefinition } from '../events/eventTypes.ts'

/**
 * Set on every schema returned by `precompileSchema`. Registered on the global symbol registry so
 * that duplicated copies of this package within one dependency tree still recognize each other's
 * work instead of compiling the same schema twice.
 */
const PRECOMPILED_SCHEMA_MARKER = Symbol.for('@message-queue-toolkit/precompiledSchema')

/**
 * Schemas that zod's fast path cannot model (async refinements, unsupported features). `compile`
 * hands those back untouched, and we have no place to put the marker without mutating a schema the
 * caller owns, so remember them here to keep repeated registrations cheap.
 */
const nonCompilableSchemas = new WeakSet<object>()

/**
 * Phantom property, never present at runtime. It exists so that a precompiled schema is
 * distinguishable from a plain one at the type level.
 */
type PrecompiledSchemaBrand = {
  readonly __mqtPrecompiled: true
}

/**
 * A schema whose validation fast path has already been built by {@link precompileSchema}.
 */
export type PrecompiledSchema<Schema> = Schema & PrecompiledSchemaBrand

/**
 * Marks an API position that takes a schema the toolkit has not seen yet. The toolkit precompiles
 * every schema it is given, so handing it an already precompiled one is duplicated work; this type
 * turns that into a compile error.
 */
export type NonPrecompiledSchema<Schema> = Schema & {
  readonly __mqtPrecompiled?: 'this schema is already precompiled, pass the original one instead'
}

/**
 * Reports whether the schema is a compiled clone produced by {@link precompileSchema}. A schema zod
 * refused to compile is not one: it is handed back as the caller's own object, which we leave alone.
 */
export function isPrecompiledSchema<Schema extends ZodType>(
  schema: Schema,
): schema is PrecompiledSchema<Schema> {
  return (schema as unknown as Record<symbol, unknown>)[PRECOMPILED_SCHEMA_MARKER] === true
}

/**
 * Builds an ahead-of-time compiled clone of the given schema, which parses noticeably faster than
 * the interpreted one. Callers of this toolkit never need to do this themselves: every schema
 * handed to a publisher, a consumer handler or an event definition is precompiled automatically.
 *
 * The original schema is left untouched, and the call is idempotent: a schema that already went
 * through it is returned as is. A schema zod refuses to compile keeps using the regular runtime
 * parser, with no observable difference for the caller.
 */
export function precompileSchema<Schema extends ZodType>(
  schema: Schema,
): PrecompiledSchema<Schema> {
  if (isPrecompiledSchema(schema)) return schema
  if (nonCompilableSchemas.has(schema)) return schema as PrecompiledSchema<Schema>

  const precompiled = compile(schema)
  if (precompiled === schema) {
    nonCompilableSchemas.add(schema)
    return schema as PrecompiledSchema<Schema>
  }

  Object.defineProperty(precompiled, PRECOMPILED_SCHEMA_MARKER, {
    value: true,
    enumerable: false,
    writable: false,
    configurable: false,
  })

  return precompiled as PrecompiledSchema<Schema>
}

/**
 * Returns a copy of the definition whose schemas are precompiled. The definition passed in is left
 * untouched, so callers keep holding the exact object they built.
 */
export function precompileEventDefinition<Definition extends CommonEventDefinition>(
  definition: Definition,
): Definition {
  return {
    ...definition,
    consumerSchema: precompileSchema(definition.consumerSchema),
    publisherSchema: precompileSchema(definition.publisherSchema),
  }
}
