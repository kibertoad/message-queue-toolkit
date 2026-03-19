import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { afterAll, afterEach, beforeAll, expect, vi } from 'vitest'
import { createTestContext, type TestContext } from '../utils/testContext.ts'
import { PermissionConsumer } from './PermissionConsumer.ts'

describe('PermissionConsumer - reconnect', () => {
  let testContext: TestContext
  let consumer: PermissionConsumer

  beforeAll(async () => {
    testContext = await createTestContext()
    consumer = new PermissionConsumer(testContext.cradle)
  })

  beforeEach(() => {
    vi.restoreAllMocks()
  })

  afterEach(async () => {
    await consumer.close()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  const simulateStreamError = () => {
    // Simulate error on pipeline
    ;(consumer as any).consumerStream.destroy()
  }

  it('should try to reconnect', async () => {
    // Given
    await consumer.init()

    const closeSpy = vi.spyOn(consumer, 'close')
    const initSpy = vi.spyOn(consumer, 'init')

    // When
    simulateStreamError()

    // Then - reconnect should trigger close and init
    await waitAndRetry(() => closeSpy.mock.calls.length === 1)
    expect(consumer.isActive).toBe(true)
    expect(consumer.isConnected).toBe(true)
    await waitAndRetry(() => initSpy.mock.calls.length === 1, 100, 15)

    expect(closeSpy).toHaveBeenCalledTimes(1)
    expect(initSpy).toHaveBeenCalledTimes(1)
    expect(consumer.isActive).toBe(true)
    expect(consumer.isConnected).toBe(true)
  })

  it('should handle errors on reconnection', { timeout: 40_000 }, async () => {
    // Given
    await consumer.init()

    const closeSpy = vi.spyOn(consumer, 'close')
    const initSpy = vi.spyOn(consumer, 'init').mockRejectedValue(new Error('Kafka unavailable'))
    const errorReporterSpy = vi.spyOn(testContext.cradle.errorReporter, 'report')

    // When - trigger stream error which starts the reconnect loop
    simulateStreamError()

    // Wait for all 5 attempts to exhaust (1+2+4+8+16 = 31s of backoff)
    await waitAndRetry(() => errorReporterSpy.mock.calls.length > 0, 500, 65)

    // Then
    expect(errorReporterSpy).toHaveBeenCalledOnce()
    expect(errorReporterSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        error: expect.objectContaining({
          message: 'Consumer failed to reconnect after max attempts',
        }),
      }),
    )

    expect(initSpy).toHaveBeenCalledTimes(5)
    expect(closeSpy).toHaveBeenCalledTimes(6) // Retries + final clean-up
    expect(consumer.isConnected).toBe(false)
    expect(consumer.isActive).toBe(false)
  })
})
