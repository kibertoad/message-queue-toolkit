import { deepEqual } from 'fast-equals'

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
  for (let key of keysSubset) {
    // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
    if (maybeSubset[key] !== maybeSuperset![key]) {
      return false
    }
  }

  return true
}

export function shallowEqual(
  object1?: Record<string, unknown>,
  object2?: Record<string, unknown>,
): boolean {
  if ((object1 && !object2) || (!object1 && object2)) {
    return false
  }
  if (!object1 && !object2) {
    return true
  }

  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  const keys1 = Object.keys(object1!)
  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  const keys2 = Object.keys(object2!)

  if (keys1.length !== keys2.length) {
    return false
  }

  // eslint-disable-next-line prefer-const
  for (let key of keys1) {
    // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
    if (object1![key] !== object2![key]) {
      return false
    }
  }

  return true
}

export function objectMatches(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  matcher: Record<string, any>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
