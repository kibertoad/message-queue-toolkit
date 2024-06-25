import { deepEqual } from 'fast-equals'

/**
 * Returns true if `maybeSubset` does not contain any fields in addition to the fields that `maybeSuperset` contain, and all of the overlapping fields are equal on a shallow level.
 */
export function isShallowSubset(
  maybeSubset?: Record<string, unknown>,
  maybeSuperset?: Record<string, unknown>,
): boolean {
  if (!maybeSubset) {
    return true
  }

  const keysSubset = Object.keys(maybeSubset)
  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  const keysSuperset = Object.keys(maybeSuperset!)

  if (keysSubset.length === 0) {
    return true
  }
  if (keysSubset.length > keysSuperset.length) {
    return false
  }

  // eslint-disable-next-line prefer-const
  for (const key of keysSubset) {
    // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
    if (maybeSubset[key] !== maybeSuperset![key]) {
      return false
    }
  }

  return true
}

/**
 * Returns true if `validatedObject` contains all of the fields included on `matcher`, and their values are deeply equal
 */
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
export function objectMatches(
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  matcher: Record<string, any>,
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  validatedObject: Record<string, any>,
): boolean {
  for (const key in matcher) {
    if (!Object.prototype.hasOwnProperty.call(matcher, key)) continue

    if (!Object.prototype.hasOwnProperty.call(validatedObject, key)) {
      return false
    }

    if (typeof matcher[key] === 'object' && matcher[key] !== null) {
      if (typeof validatedObject[key] !== 'object' || validatedObject[key] === null) {
        return false
      }

      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
      if (!objectMatches(matcher[key], validatedObject[key])) {
        return false
      }
      // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    } else if (!deepEqual(matcher[key], validatedObject[key])) {
      return false
    }
  }
  return true
}
