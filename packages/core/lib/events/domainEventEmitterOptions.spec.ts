import { describe, expect, it } from 'vitest'
import { resolveRegistrationOptions, resolveRetryOptions } from './domainEventEmitterOptions.ts'

const DEFAULTS = { maxRetries: 0, baseRetryDelayMs: 100, maxRetryDelayMs: 1000 }

describe('domainEventEmitterOptions', () => {
  describe('resolveRegistrationOptions', () => {
    it('returns an options object as is', () => {
      const options = { isBackgroundHandler: true, retry: { maxRetries: 2 } }

      expect(resolveRegistrationOptions(options)).toBe(options)
    })

    it('treats a boolean as the isBackgroundHandler shorthand', () => {
      expect(resolveRegistrationOptions(true)).toEqual({ isBackgroundHandler: true })
      expect(resolveRegistrationOptions(false)).toEqual({ isBackgroundHandler: false })
    })

    it.each([5, 0, 'background', '', () => {}, [], Symbol('x'), null])(
      'rejects %p, which would otherwise register a foreground handler silently',
      (options) => {
        expect(() =>
          // @ts-expect-error covering JS callers that ignore the type
          resolveRegistrationOptions(options),
        ).toThrowError(/registration options must be an object or a boolean/)
      },
    )
  })

  describe('resolveRetryOptions', () => {
    it('falls back to library defaults when nothing is provided', () => {
      expect(resolveRetryOptions()).toEqual({ ...DEFAULTS, isRetryable: undefined })
      expect(resolveRetryOptions({})).toEqual({ ...DEFAULTS, isRetryable: undefined })
    })

    it('fills in only the options that are missing', () => {
      expect(resolveRetryOptions({ maxRetries: 3 })).toMatchObject({
        maxRetries: 3,
        baseRetryDelayMs: DEFAULTS.baseRetryDelayMs,
        maxRetryDelayMs: DEFAULTS.maxRetryDelayMs,
      })
    })

    it('treats an explicitly undefined option as not provided', () => {
      expect(resolveRetryOptions({ maxRetries: undefined })).toMatchObject({
        maxRetries: DEFAULTS.maxRetries,
      })
    })

    it('keeps a provided isRetryable predicate', () => {
      const isRetryable = () => true

      expect(resolveRetryOptions({ isRetryable })).toMatchObject({ isRetryable })
    })

    it.each([Number.POSITIVE_INFINITY, Number.NaN, 1.5, -1, 6])(
      'rejects maxRetries %p',
      (maxRetries) => {
        expect(() => resolveRetryOptions({ maxRetries })).toThrowError(
          /maxRetries must be an integer between 0 and 5/,
        )
      },
    )

    it('accepts the highest allowed maxRetries', () => {
      expect(resolveRetryOptions({ maxRetries: 5 })).toMatchObject({ maxRetries: 5 })
    })

    it.each([-1, Number.NaN, Number.POSITIVE_INFINITY, 10_001])(
      'rejects baseRetryDelayMs %p',
      (baseRetryDelayMs) => {
        expect(() => resolveRetryOptions({ baseRetryDelayMs })).toThrowError(
          /baseRetryDelayMs must be between 0 and 10000 ms/,
        )
      },
    )

    it.each([-1, Number.NaN, Number.POSITIVE_INFINITY, 10_001])(
      'rejects maxRetryDelayMs %p',
      (maxRetryDelayMs) => {
        expect(() => resolveRetryOptions({ maxRetryDelayMs })).toThrowError(
          /maxRetryDelayMs must be between 0 and 10000 ms/,
        )
      },
    )

    it('rejects a base delay above the max delay, which would flatten the backoff', () => {
      expect(() =>
        resolveRetryOptions({ baseRetryDelayMs: 5000, maxRetryDelayMs: 1000 }),
      ).toThrowError(/baseRetryDelayMs \(5000\) must not exceed maxRetryDelayMs \(1000\)/)
    })

    it('rejects a non-function isRetryable', () => {
      expect(() =>
        // @ts-expect-error covering JS callers that ignore the type
        resolveRetryOptions({ isRetryable: 'yes' }),
      ).toThrowError(/isRetryable must be a function/)
    })

    it('rejects an async isRetryable, which would always be truthy', () => {
      expect(() =>
        // @ts-expect-error covering JS callers that ignore the type
        resolveRetryOptions({ isRetryable: async () => true }),
      ).toThrowError(/isRetryable must be synchronous/)
    })
  })
})
