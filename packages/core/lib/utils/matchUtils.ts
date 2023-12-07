import { deepEqual } from 'fast-equals'

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
