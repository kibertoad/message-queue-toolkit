import {
  copyWithoutUndefined,
  InternalError,
  isObject,
  stringValueSerializer,
} from '@lokalise/node-core'

/**
 * Upper bound for `maxRetries`. Retries are awaited by `dispose()`, so an unbounded budget would
 * keep a permanently failing handler holding shutdown open.
 */
const MAX_ALLOWED_RETRIES = 5

/**
 * Upper bound for a single backoff delay. `dispose()` waits for the backoff to elapse, so the
 * budget is kept small on purpose
 */
const MAX_ALLOWED_RETRY_DELAY_MS = 10 * 1000

const DEFAULT_HANDLER_RETRY_OPTIONS = {
  maxRetries: 0,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 1000,
} as const

/**
 * Retries are performed in-memory, within the same dispatch, so they do not survive a process
 * restart and the handler must be idempotent. `dispose()` waits for them to finish, so a large
 * retry budget directly extends graceful shutdown; keep it small.
 */
export type EventHandlerRetryOptions = {
  /**
   * Number of extra attempts after the initial one. Values above 5 are rejected.
   * @default 0
   */
  maxRetries?: number
  /**
   * Base delay for the exponential backoff.
   * Actual delay = min(baseRetryDelayMs * 2 ^ attempt, maxRetryDelayMs).
   * Values above 10 seconds are rejected.
   * @default 100
   */
  baseRetryDelayMs?: number
  /**
   * Upper bound for the delay between attempts. Values above 10 seconds, or below
   * `baseRetryDelayMs`, are rejected.
   * @default 1000
   */
  maxRetryDelayMs?: number
  /**
   * Decides whether an error is worth retrying. Every error is retried by default.
   */
  isRetryable?: (error: unknown) => boolean
}

export type EventHandlerRegistrationOptions = {
  isBackgroundHandler?: boolean
  retry?: EventHandlerRetryOptions
}

export type ResolvedRetryOptions = Required<Omit<EventHandlerRetryOptions, 'isRetryable'>> &
  Pick<EventHandlerRetryOptions, 'isRetryable'>

export const resolveRegistrationOptions = (
  options: boolean | EventHandlerRegistrationOptions,
): EventHandlerRegistrationOptions => {
  if (isObject(options) && !Array.isArray(options)) {
    return options
  }

  if (typeof options === 'boolean') {
    return { isBackgroundHandler: options }
  }

  throw new InternalError({
    errorCode: 'INVALID_REGISTRATION_OPTIONS',
    message: `registration options must be an object or a boolean, received ${stringValueSerializer(options)}`,
  })
}

export const resolveRetryOptions = (retry?: EventHandlerRetryOptions): ResolvedRetryOptions => {
  const resolved: ResolvedRetryOptions = {
    ...DEFAULT_HANDLER_RETRY_OPTIONS,
    ...copyWithoutUndefined(retry ?? {}),
  }

  /*
    Rejected at registration rather than at dispatch time: a non-integer `maxRetries` silently
    changes how many attempts are made, and an unbounded one would keep a permanently failing
    handler in the retry loop, holding `dispose()` open with it.
  */
  if (
    !Number.isInteger(resolved.maxRetries) ||
    resolved.maxRetries < 0 ||
    resolved.maxRetries > MAX_ALLOWED_RETRIES
  ) {
    throw new InternalError({
      errorCode: 'INVALID_RETRY_OPTIONS',
      message: `maxRetries must be an integer between 0 and ${MAX_ALLOWED_RETRIES}, received ${resolved.maxRetries}`,
    })
  }
  for (const field of ['baseRetryDelayMs', 'maxRetryDelayMs'] as const) {
    if (
      !Number.isFinite(resolved[field]) ||
      resolved[field] < 0 ||
      resolved[field] > MAX_ALLOWED_RETRY_DELAY_MS
    ) {
      throw new InternalError({
        errorCode: 'INVALID_RETRY_OPTIONS',
        message: `${field} must be between 0 and ${MAX_ALLOWED_RETRY_DELAY_MS} ms, received ${resolved[field]}`,
      })
    }
  }

  if (resolved.baseRetryDelayMs > resolved.maxRetryDelayMs) {
    throw new InternalError({
      errorCode: 'INVALID_RETRY_OPTIONS',
      message: `baseRetryDelayMs (${resolved.baseRetryDelayMs}) must not exceed maxRetryDelayMs (${resolved.maxRetryDelayMs}), otherwise every delay collapses to maxRetryDelayMs`,
    })
  }

  if (resolved.isRetryable !== undefined) {
    if (typeof resolved.isRetryable !== 'function') {
      throw new InternalError({
        errorCode: 'INVALID_RETRY_OPTIONS',
        message: 'isRetryable must be a function',
      })
    }
    /*
      An async predicate always returns a truthy promise, which would silently turn it into
      "retry everything", the opposite of what a predicate excluding some errors is written for.
    */
    if (resolved.isRetryable.constructor.name === 'AsyncFunction') {
      throw new InternalError({
        errorCode: 'INVALID_RETRY_OPTIONS',
        message: 'isRetryable must be synchronous',
      })
    }
  }

  return resolved
}
