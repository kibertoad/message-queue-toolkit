import { setTimeout } from 'node:timers/promises'
import { type CommonLogger, type ErrorReporter, isError } from '@lokalise/node-core'
import { NO_TIMEOUT, type StartupResourcePollingConfig } from '../types/queueOptionsTypes.ts'

const DEFAULT_POLLING_INTERVAL_MS = 5000

/**
 * Context passed to error callbacks indicating whether the error is final.
 */
export type PollingErrorContext = {
  /**
   * If true, the operation has stopped and will not retry.
   * If false, this is a transient error and the operation will continue.
   */
  isFinal: boolean
}

/**
 * Callback invoked when a polling operation fails.
 */
export type PollingErrorCallback = (error: Error, context: PollingErrorContext) => void

export type StartupResourcePollingCheckResult<T> =
  | {
      isAvailable: true
      result: T
    }
  | {
      isAvailable: false
    }

export type WaitForResourceOptions<T> = {
  /**
   * Startup resource polling configuration
   */
  config: StartupResourcePollingConfig

  /**
   * Function that checks if the resource is available.
   * Should return { isAvailable: true, result: T } when resource exists,
   * or { isAvailable: false } when resource doesn't exist.
   * Should throw on unexpected errors.
   */
  checkFn: () => Promise<StartupResourcePollingCheckResult<T>>

  /**
   * Human-readable name of the resource for logging
   */
  resourceName: string

  /**
   * Logger instance for progress logging
   */
  logger?: CommonLogger

  /**
   * Error reporter for reporting timeout errors when throwOnTimeout is false.
   * Required when config.throwOnTimeout is false.
   */
  errorReporter?: ErrorReporter

  /**
   * Callback invoked when resource becomes available in non-blocking mode.
   * Only used when config.nonBlocking is true and the resource was not immediately available.
   */
  onResourceAvailable?: (result: T) => void

  /**
   * Callback invoked when background polling fails in non-blocking mode.
   * This can happen due to polling timeout or unexpected errors during polling.
   * Only used when config.nonBlocking is true.
   */
  onError?: PollingErrorCallback
}

export class StartupResourcePollingTimeoutError extends Error {
  public readonly resourceName: string
  public readonly timeoutMs: number

  constructor(resourceName: string, timeoutMs: number) {
    super(
      `Timeout waiting for resource "${resourceName}" to become available after ${timeoutMs}ms. ` +
        'The resource may not exist or there may be a configuration issue.',
    )
    this.name = 'StartupResourcePollingTimeoutError'
    this.resourceName = resourceName
    this.timeoutMs = timeoutMs
  }
}

type TimeoutCheckResult =
  | { exceeded: false }
  | { exceeded: true; error: StartupResourcePollingTimeoutError }

function checkTimeoutExceeded(
  hasTimeout: boolean,
  timeoutMs: number,
  startTime: number,
  resourceName: string,
  attemptCount: number,
  logger?: CommonLogger,
): TimeoutCheckResult {
  if (!hasTimeout) return { exceeded: false }

  const elapsedMs = Date.now() - startTime
  if (elapsedMs >= timeoutMs) {
    logger?.error({
      message: `Timeout waiting for resource "${resourceName}" to become available`,
      resourceName,
      timeoutMs,
      attemptCount,
      elapsedMs,
    })
    return {
      exceeded: true,
      error: new StartupResourcePollingTimeoutError(resourceName, timeoutMs),
    }
  }
  return { exceeded: false }
}

type HandleTimeoutParams = {
  timeoutResult: TimeoutCheckResult & { exceeded: true }
  throwOnTimeout: boolean
  resourceName: string
  timeoutMs: number
  attemptCount: number
  timeoutCount: number
  logger?: CommonLogger
  errorReporter?: ErrorReporter
}

/**
 * Handles timeout condition - either throws or reports and returns new timeout count.
 * @returns Updated timeout count after handling
 */
function handleTimeout(params: HandleTimeoutParams): number {
  const {
    timeoutResult,
    throwOnTimeout,
    resourceName,
    timeoutMs,
    attemptCount,
    timeoutCount,
    logger,
    errorReporter,
  } = params

  if (throwOnTimeout) {
    throw timeoutResult.error
  }

  // Report error and reset timeout counter
  const newTimeoutCount = timeoutCount + 1
  errorReporter?.report({
    error: timeoutResult.error,
    context: {
      resourceName,
      timeoutMs,
      attemptCount,
      timeoutCount: newTimeoutCount,
    },
  })
  logger?.warn({
    message: `Timeout waiting for resource "${resourceName}", resetting timeout counter and continuing`,
    resourceName,
    timeoutMs,
    attemptCount,
    timeoutCount: newTimeoutCount,
  })
  return newTimeoutCount
}

function logResourceAvailable(
  resourceName: string,
  attemptCount: number,
  startTime: number,
  logger?: CommonLogger,
): void {
  const elapsedMs = Date.now() - startTime
  logger?.info({
    message: `Resource "${resourceName}" is now available`,
    resourceName,
    attemptCount,
    elapsedMs,
  })
}

/**
 * Internal polling loop that waits for a resource to become available.
 * Separated from main function to support non-blocking mode.
 */
async function pollForResource<T>(
  options: WaitForResourceOptions<T>,
  pollingIntervalMs: number,
  hasTimeout: boolean,
  timeoutMs: number,
  throwOnTimeout: boolean,
  initialAttemptCount: number,
): Promise<T> {
  const { checkFn, resourceName, logger, errorReporter } = options

  let startTime = Date.now()
  let attemptCount = initialAttemptCount
  let timeoutCount = 0

  while (true) {
    attemptCount++
    const timeoutResult = checkTimeoutExceeded(
      hasTimeout,
      timeoutMs,
      startTime,
      resourceName,
      attemptCount,
      logger,
    )

    if (timeoutResult.exceeded) {
      timeoutCount = handleTimeout({
        timeoutResult,
        throwOnTimeout,
        resourceName,
        timeoutMs,
        attemptCount,
        timeoutCount,
        logger,
        errorReporter,
      })
      startTime = Date.now()
    }

    const result = await checkFn().catch((error) => {
      // Unexpected error during check - log and rethrow
      logger?.error({
        message: `Error checking resource availability for "${resourceName}"`,
        resourceName,
        error,
        attemptCount,
      })
      throw error
    })

    if (result.isAvailable) {
      logResourceAvailable(resourceName, attemptCount, startTime, logger)
      return result.result
    }

    // Resource not available yet, log and wait
    if (attemptCount === 1 || attemptCount % 12 === 0) {
      // Log on first attempt and then every minute (assuming 5s interval)
      const elapsedMs = Date.now() - startTime
      logger?.debug({
        message: `Resource "${resourceName}" not available yet, will retry`,
        resourceName,
        attemptCount,
        elapsedMs,
        nextRetryInMs: pollingIntervalMs,
      })
    }

    // Wait before next attempt
    await setTimeout(pollingIntervalMs)
  }
}

/**
 * Waits for a resource to become available by polling.
 * This is used for startup resource polling mode where resources may not exist at startup.
 *
 * @param options - Configuration and check function
 * @returns The result from the check function when resource becomes available,
 *          or undefined if nonBlocking is true and resource was not immediately available
 * @throws StartupResourcePollingTimeoutError if timeout is reached and throwOnTimeout is true (default)
 */
export async function waitForResource<T>(
  options: WaitForResourceOptions<T>,
): Promise<T | undefined> {
  const { config, checkFn, resourceName, logger, onResourceAvailable } = options
  const pollingIntervalMs = config.pollingIntervalMs ?? DEFAULT_POLLING_INTERVAL_MS
  const hasTimeout = config.timeoutMs !== NO_TIMEOUT
  const timeoutMs = hasTimeout ? (config.timeoutMs as number) : 0
  const throwOnTimeout = config.throwOnTimeout !== false
  const nonBlocking = config.nonBlocking === true

  logger?.info({
    message: `Waiting for resource "${resourceName}" to become available`,
    resourceName,
    pollingIntervalMs,
    timeoutMs: hasTimeout ? timeoutMs : 'NO_TIMEOUT',
    throwOnTimeout,
    nonBlocking,
  })

  // First check - always done synchronously
  const initialResult = await checkFn().catch((error) => {
    logger?.error({
      message: `Error checking resource availability for "${resourceName}"`,
      resourceName,
      error,
      attemptCount: 1,
    })
    throw error
  })

  if (initialResult.isAvailable) {
    logResourceAvailable(resourceName, 1, Date.now(), logger)
    return initialResult.result
  }

  // Resource not available on first check
  if (nonBlocking) {
    // Start background polling and return immediately
    logger?.info({
      message: `Resource "${resourceName}" not immediately available, starting background polling`,
      resourceName,
      pollingIntervalMs,
    })

    // Fire and forget - start polling in background
    const { onError } = options
    setTimeout(pollingIntervalMs).then(() => {
      pollForResource(options, pollingIntervalMs, hasTimeout, timeoutMs, throwOnTimeout, 1)
        .then((result) => {
          onResourceAvailable?.(result)
        })
        .catch((err) => {
          const error = isError(err) ? err : new Error(String(err))
          logger?.error({
            message: `Background polling for resource "${resourceName}" failed`,
            resourceName,
            error,
          })
          // isFinal: true because pollForResource only throws when it gives up
          // (timeout with throwOnTimeout: true, or unexpected error)
          onError?.(error, { isFinal: true })
        })
    })

    return undefined
  }

  // Blocking mode - continue polling and wait for result
  return pollForResource(options, pollingIntervalMs, hasTimeout, timeoutMs, throwOnTimeout, 1)
}

/**
 * Helper to check if startup resource polling is enabled.
 * Returns true only when config is provided and enabled is explicitly true.
 */
export function isStartupResourcePollingEnabled(
  config?: StartupResourcePollingConfig,
): config is StartupResourcePollingConfig {
  return config?.enabled === true
}
