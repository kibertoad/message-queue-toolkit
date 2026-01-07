import { setTimeout } from 'node:timers/promises'
import type { CommonLogger } from '@lokalise/node-core'
import type { ResourceAvailabilityConfig } from '../types/queueOptionsTypes.ts'

const DEFAULT_POLLING_INTERVAL_MS = 5000

export type ResourceAvailabilityCheckResult<T> =
  | {
      isAvailable: true
      result: T
    }
  | {
      isAvailable: false
    }

export type WaitForResourceOptions<T> = {
  /**
   * Resource availability configuration
   */
  config: ResourceAvailabilityConfig

  /**
   * Function that checks if the resource is available.
   * Should return { isAvailable: true, result: T } when resource exists,
   * or { isAvailable: false } when resource doesn't exist.
   * Should throw on unexpected errors.
   */
  checkFn: () => Promise<ResourceAvailabilityCheckResult<T>>

  /**
   * Human-readable name of the resource for logging
   */
  resourceName: string

  /**
   * Logger instance for progress logging
   */
  logger?: CommonLogger
}

export class ResourceAvailabilityTimeoutError extends Error {
  public readonly resourceName: string
  public readonly timeoutMs: number

  constructor(resourceName: string, timeoutMs: number) {
    super(
      `Timeout waiting for resource "${resourceName}" to become available after ${timeoutMs}ms. ` +
        'The resource may not exist or there may be a configuration issue.',
    )
    this.name = 'ResourceAvailabilityTimeoutError'
    this.resourceName = resourceName
    this.timeoutMs = timeoutMs
  }
}

function checkTimeoutExceeded(
  hasTimeout: boolean,
  timeoutMs: number | undefined,
  startTime: number,
  resourceName: string,
  attemptCount: number,
  logger?: CommonLogger,
): void {
  if (!hasTimeout || timeoutMs === undefined) return

  const elapsedMs = Date.now() - startTime
  if (elapsedMs >= timeoutMs) {
    logger?.error({
      message: `Timeout waiting for resource "${resourceName}" to become available`,
      resourceName,
      timeoutMs,
      attemptCount,
      elapsedMs,
    })
    throw new ResourceAvailabilityTimeoutError(resourceName, timeoutMs)
  }
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
 * Waits for a resource to become available by polling.
 * This is used for eventual consistency mode where resources may not exist at startup.
 *
 * @param options - Configuration and check function
 * @returns The result from the check function when resource becomes available
 * @throws ResourceAvailabilityTimeoutError if timeout is reached
 */
export async function waitForResource<T>(options: WaitForResourceOptions<T>): Promise<T> {
  const { config, checkFn, resourceName, logger } = options
  const pollingIntervalMs = config.pollingIntervalMs ?? DEFAULT_POLLING_INTERVAL_MS
  const timeoutMs = config.timeoutMs
  const hasTimeout = timeoutMs !== undefined && timeoutMs > 0

  const startTime = Date.now()
  let attemptCount = 0

  logger?.info({
    message: `Waiting for resource "${resourceName}" to become available`,
    resourceName,
    pollingIntervalMs,
    timeoutMs: timeoutMs ?? 'indefinite',
  })

  while (true) {
    attemptCount++
    checkTimeoutExceeded(hasTimeout, timeoutMs, startTime, resourceName, attemptCount, logger)

    try {
      const result = await checkFn()

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
    } catch (error) {
      // Unexpected error during check - log and rethrow
      logger?.error({
        message: `Error checking resource availability for "${resourceName}"`,
        resourceName,
        error,
        attemptCount,
      })
      throw error
    }

    // Wait before next attempt
    await setTimeout(pollingIntervalMs)
  }
}

/**
 * Helper to check if resource availability waiting is enabled
 */
export function isResourceAvailabilityWaitingEnabled(
  config?: ResourceAvailabilityConfig,
): config is ResourceAvailabilityConfig {
  return config?.enabled === true
}
