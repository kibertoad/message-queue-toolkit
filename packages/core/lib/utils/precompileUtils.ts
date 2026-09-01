import { compile, type ZodType } from 'zod/v4'
import type { CommonEventDefinition } from '../events/eventTypes.ts'

/**
 * What `precompileSchema` has already produced, keyed both on the schema the caller owns and on
 * the result itself, so a source schema and its clone resolve to the same clone.
 *
 * The same schema routinely reaches the toolkit from several directions: an event definition
 * registered on an `EventRegistry`, then handed to a publisher, then to a consumer handler.
 * Compilation costs real time, so it happens once per schema and every later registration is a
 * lookup. Weak, so schemas that go out of use stay collectable.
 */
const precompiledSchemas = new WeakMap<ZodType, ZodType>()

/**
 * Same memo, for whole event definitions. Keeps `precompileEventDefinition` from producing a new
 * copy of the definition on every registration.
 */
const precompiledDefinitions = new WeakMap<CommonEventDefinition, CommonEventDefinition>()

/**
 * Builds an ahead-of-time compiled clone of the given schema, which parses noticeably faster than
 * the interpreted one. Callers of this toolkit never need to do this themselves: every schema
 * handed to a publisher, a consumer handler or an event definition is precompiled automatically.
 *
 * The original schema is left untouched, and repeat calls are free: the same schema always yields
 * the same clone, and passing a clone back returns it as is. A schema zod refuses to compile keeps
 * using the regular runtime parser, with no observable difference for the caller.
 */
export function precompileSchema<Schema extends ZodType>(schema: Schema): Schema {
  const memoized = precompiledSchemas.get(schema)
  if (memoized) return memoized as Schema

  // `compile` never throws: it hands the schema back untouched when its fast path cannot model
  // one of the features in use. Memoizing that outcome keeps later registrations from re-running
  // codegen that will refuse again.
  const precompiled = compile(schema)

  precompiledSchemas.set(schema, precompiled)
  precompiledSchemas.set(precompiled, precompiled)

  return precompiled
}

/**
 * Returns a copy of the definition whose schemas are precompiled. The definition passed in is left
 * untouched, so callers keep holding the exact object they built.
 *
 * Both schemas are compiled, because both are read on a hot path: `DomainEventEmitter` parses with
 * `publisherSchema`, while `AbstractPublisherManager` registers `consumerSchema` as the schema its
 * publishers parse with.
 */
export function precompileEventDefinition<Definition extends CommonEventDefinition>(
  definition: Definition,
): Definition {
  const memoized = precompiledDefinitions.get(definition)
  if (memoized) return memoized as Definition

  const precompiled = {
    ...definition,
    consumerSchema: precompileSchema(definition.consumerSchema),
    publisherSchema: precompileSchema(definition.publisherSchema),
  }

  precompiledDefinitions.set(definition, precompiled)
  precompiledDefinitions.set(precompiled, precompiled)

  return precompiled
}
