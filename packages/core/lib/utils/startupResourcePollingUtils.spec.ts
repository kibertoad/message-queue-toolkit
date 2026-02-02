import { describe, expect, it, vi } from 'vitest'
import { NO_TIMEOUT } from '../types/queueOptionsTypes.ts'
import {
  isStartupResourcePollingEnabled,
  StartupResourcePollingTimeoutError,
  waitForResource,
} from './startupResourcePollingUtils.ts'

describe('startupResourcePollingUtils', () => {
  describe('isStartupResourcePollingEnabled', () => {
    it('returns true when enabled is true', () => {
      expect(isStartupResourcePollingEnabled({ enabled: true, timeoutMs: 5000 })).toBe(true)
    })

    it('returns false when enabled is not specified', () => {
      expect(isStartupResourcePollingEnabled({ timeoutMs: 5000 })).toBe(false)
      expect(isStartupResourcePollingEnabled({ pollingIntervalMs: 1000, timeoutMs: 5000 })).toBe(
        false,
      )
    })

    it('returns false when enabled is false', () => {
      expect(isStartupResourcePollingEnabled({ enabled: false, timeoutMs: 5000 })).toBe(false)
    })

    it('returns false when config is undefined', () => {
      expect(isStartupResourcePollingEnabled(undefined)).toBe(false)
    })
  })

  describe('waitForResource', () => {
    it('returns immediately when resource is available on first check', async () => {
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: true, result: 'test-result' })

      const result = await waitForResource({
        config: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000 },
        checkFn,
        resourceName: 'test-resource',
      })

      expect(result).toBe('test-result')
      expect(checkFn).toHaveBeenCalledTimes(1)
    })

    it('polls until resource becomes available', async () => {
      let callCount = 0
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount < 3) {
          return Promise.resolve({ isAvailable: false })
        }
        return Promise.resolve({ isAvailable: true, result: 'test-result' })
      })

      const result = await waitForResource({
        config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 5000 },
        checkFn,
        resourceName: 'test-resource',
      })

      expect(result).toBe('test-result')
      expect(checkFn).toHaveBeenCalledTimes(3)
    })

    it('throws StartupResourcePollingTimeoutError when timeout is reached', async () => {
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: false })

      await expect(
        waitForResource({
          config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 50 },
          checkFn,
          resourceName: 'test-resource',
        }),
      ).rejects.toThrow(StartupResourcePollingTimeoutError)

      // Should have made at least a few attempts
      expect(checkFn.mock.calls.length).toBeGreaterThan(0)
    })

    it('throws the original error when checkFn throws', async () => {
      const checkFn = vi.fn().mockRejectedValue(new Error('Unexpected error'))

      await expect(
        waitForResource({
          config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 5000 },
          checkFn,
          resourceName: 'test-resource',
        }),
      ).rejects.toThrow('Unexpected error')

      expect(checkFn).toHaveBeenCalledTimes(1)
    })

    it('uses default polling interval when not specified', async () => {
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: true, result: 'test-result' })

      const result = await waitForResource({
        config: { enabled: true, timeoutMs: 5000 },
        checkFn,
        resourceName: 'test-resource',
      })

      expect(result).toBe('test-result')
    })

    it('logs progress when logger is provided', async () => {
      const logger = {
        info: vi.fn(),
        debug: vi.fn(),
        error: vi.fn(),
      }
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: true, result: 'test-result' })

      await waitForResource({
        config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 5000 },
        checkFn,
        resourceName: 'test-resource',
        // @ts-expect-error - partial logger for testing
        logger,
      })

      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({
          message: expect.stringContaining('Waiting for resource'),
          resourceName: 'test-resource',
        }),
      )
      expect(logger.info).toHaveBeenCalledWith(
        expect.objectContaining({
          message: expect.stringContaining('is now available'),
          resourceName: 'test-resource',
        }),
      )
    })

    it('polls indefinitely when NO_TIMEOUT is used', async () => {
      let callCount = 0
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount < 5) {
          return Promise.resolve({ isAvailable: false })
        }
        return Promise.resolve({ isAvailable: true, result: 'test-result' })
      })

      const result = await waitForResource({
        config: {
          enabled: true,
          pollingIntervalMs: 1,
          timeoutMs: NO_TIMEOUT,
        },
        checkFn,
        resourceName: 'test-resource',
      })

      expect(result).toBe('test-result')
      expect(checkFn).toHaveBeenCalledTimes(5)
    })

    it('throws by default when throwOnTimeout is not specified', async () => {
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: false })

      await expect(
        waitForResource({
          config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 50 },
          checkFn,
          resourceName: 'test-resource',
        }),
      ).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('throws when throwOnTimeout is true', async () => {
      const checkFn = vi.fn().mockResolvedValue({ isAvailable: false })

      await expect(
        waitForResource({
          config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 50, throwOnTimeout: true },
          checkFn,
          resourceName: 'test-resource',
        }),
      ).rejects.toThrow(StartupResourcePollingTimeoutError)
    })

    it('reports error and continues polling when throwOnTimeout is false', async () => {
      let callCount = 0
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++
        // Become available after what would be 2 timeout cycles
        if (callCount < 8) {
          return Promise.resolve({ isAvailable: false })
        }
        return Promise.resolve({ isAvailable: true, result: 'test-result' })
      })

      const errorReporter = {
        report: vi.fn(),
      }

      const result = await waitForResource({
        config: {
          enabled: true,
          pollingIntervalMs: 10,
          timeoutMs: 35, // Will timeout around every 3-4 calls
          throwOnTimeout: false,
        },
        checkFn,
        resourceName: 'test-resource',
        errorReporter,
      })

      expect(result).toBe('test-result')
      // Should have reported at least one timeout error
      expect(errorReporter.report).toHaveBeenCalled()
      expect(errorReporter.report).toHaveBeenCalledWith(
        expect.objectContaining({
          error: expect.any(StartupResourcePollingTimeoutError),
          context: expect.objectContaining({
            resourceName: 'test-resource',
            timeoutMs: 35,
          }),
        }),
      )
    })

    it('logs warning when throwOnTimeout is false and timeout is reached', async () => {
      let callCount = 0
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++
        if (callCount < 5) {
          return Promise.resolve({ isAvailable: false })
        }
        return Promise.resolve({ isAvailable: true, result: 'test-result' })
      })

      const logger = {
        info: vi.fn(),
        debug: vi.fn(),
        error: vi.fn(),
        warn: vi.fn(),
      }

      await waitForResource({
        config: {
          enabled: true,
          pollingIntervalMs: 10,
          timeoutMs: 25,
          throwOnTimeout: false,
        },
        checkFn,
        resourceName: 'test-resource',
        // @ts-expect-error - partial logger for testing
        logger,
      })

      expect(logger.warn).toHaveBeenCalledWith(
        expect.objectContaining({
          message: expect.stringContaining('resetting timeout counter'),
          resourceName: 'test-resource',
        }),
      )
    })

    it('tracks timeout count when throwOnTimeout is false', async () => {
      let callCount = 0
      const checkFn = vi.fn().mockImplementation(() => {
        callCount++
        // Become available after multiple timeout cycles
        if (callCount < 12) {
          return Promise.resolve({ isAvailable: false })
        }
        return Promise.resolve({ isAvailable: true, result: 'test-result' })
      })

      const errorReporter = {
        report: vi.fn(),
      }

      await waitForResource({
        config: {
          enabled: true,
          pollingIntervalMs: 5,
          timeoutMs: 20, // Will timeout every ~4 calls
          throwOnTimeout: false,
        },
        checkFn,
        resourceName: 'test-resource',
        errorReporter,
      })

      // Should have reported multiple timeout errors with increasing timeoutCount
      expect(errorReporter.report.mock.calls.length).toBeGreaterThan(1)

      // Verify timeoutCount increments
      const firstCall = errorReporter.report.mock.calls[0]?.[0] as {
        context: { timeoutCount: number }
      }
      const secondCall = errorReporter.report.mock.calls[1]?.[0] as {
        context: { timeoutCount: number }
      }
      expect(firstCall.context.timeoutCount).toBe(1)
      expect(secondCall.context.timeoutCount).toBe(2)
    })

    describe('nonBlocking mode', () => {
      it('returns result immediately when resource is available on first check', async () => {
        const checkFn = vi.fn().mockResolvedValue({ isAvailable: true, result: 'test-result' })
        const onResourceAvailable = vi.fn()

        const result = await waitForResource({
          config: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000, nonBlocking: true },
          checkFn,
          resourceName: 'test-resource',
          onResourceAvailable,
        })

        expect(result).toBe('test-result')
        expect(checkFn).toHaveBeenCalledTimes(1)
        expect(onResourceAvailable).not.toHaveBeenCalled()
      })

      it('returns undefined immediately when resource is not available and starts background polling', async () => {
        const checkFn = vi.fn().mockResolvedValue({ isAvailable: false })
        const onResourceAvailable = vi.fn()

        const result = await waitForResource({
          config: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000, nonBlocking: true },
          checkFn,
          resourceName: 'test-resource',
          onResourceAvailable,
        })

        expect(result).toBeUndefined()
        expect(checkFn).toHaveBeenCalledTimes(1)
      })

      it('calls onResourceAvailable when resource becomes available in background', async () => {
        let callCount = 0
        const checkFn = vi.fn().mockImplementation(() => {
          callCount++
          if (callCount < 3) {
            return Promise.resolve({ isAvailable: false })
          }
          return Promise.resolve({ isAvailable: true, result: 'test-result' })
        })
        const onResourceAvailable = vi.fn()

        const result = await waitForResource({
          config: { enabled: true, pollingIntervalMs: 10, timeoutMs: 5000, nonBlocking: true },
          checkFn,
          resourceName: 'test-resource',
          onResourceAvailable,
        })

        expect(result).toBeUndefined()

        // Wait for background polling to complete
        await new Promise((resolve) => globalThis.setTimeout(resolve, 100))

        expect(onResourceAvailable).toHaveBeenCalledWith('test-result')
      })

      it('logs info message when starting background polling', async () => {
        const checkFn = vi.fn().mockResolvedValue({ isAvailable: false })
        const logger = {
          info: vi.fn(),
          debug: vi.fn(),
          error: vi.fn(),
          warn: vi.fn(),
        }

        await waitForResource({
          config: { enabled: true, pollingIntervalMs: 100, timeoutMs: 5000, nonBlocking: true },
          checkFn,
          resourceName: 'test-resource',
          // @ts-expect-error - partial logger for testing
          logger,
        })

        expect(logger.info).toHaveBeenCalledWith(
          expect.objectContaining({
            message: expect.stringContaining('starting background polling'),
            resourceName: 'test-resource',
          }),
        )
      })
    })
  })

  describe('StartupResourcePollingTimeoutError', () => {
    it('includes resource name and timeout in message', () => {
      const error = new StartupResourcePollingTimeoutError('my-queue', 5000)

      expect(error.message).toContain('my-queue')
      expect(error.message).toContain('5000')
      expect(error.resourceName).toBe('my-queue')
      expect(error.timeoutMs).toBe(5000)
      expect(error.name).toBe('StartupResourcePollingTimeoutError')
    })
  })
})
