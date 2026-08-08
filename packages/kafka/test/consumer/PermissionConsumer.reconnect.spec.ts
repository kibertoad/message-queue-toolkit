import { setTimeout } from 'node:timers/promises'
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

  it('should not reconnect when the failing stream is no longer the active one', async () => {
    // Given
    await consumer.init()
    const reconnectSpy = vi.spyOn(consumer as any, 'reconnect')
    const staleStream = (consumer as any).consumerStream

    // When - the stream fails after it stopped being the active one, the way it does once
    // close() or a reconnect has replaced it
    ;(consumer as any).consumerStream = undefined
    staleStream.destroy()
    await setTimeout(500)

    // Then
    expect(reconnectSpy).not.toHaveBeenCalled()
  })

  it('should not reconnect while the consumer is deliberately closing', async () => {
    // Given
    await consumer.init()
    const reconnectSpy = vi.spyOn(consumer as any, 'reconnect')

    // When - the stream fails during a close, which is what emits `Premature close`
    ;(consumer as any).isClosing = true
    simulateStreamError()
    await setTimeout(500)
    ;(consumer as any).isClosing = false

    // Then
    expect(reconnectSpy).not.toHaveBeenCalled()
  })

  it('should reuse the in-flight initialization when init() is called concurrently', async () => {
    // Given
    const doInitSpy = vi.spyOn(consumer as any, 'doInit')

    // When
    await Promise.all([consumer.init(), consumer.init(), consumer.init()])

    // Then - a concurrent caller must not be handed a resolved init() before the stream is up
    expect(doInitSpy).toHaveBeenCalledOnce()
    expect((consumer as any).consumerStream).toBeDefined()
    expect(consumer.isConnected).toBe(true)
  })

  it('should wait for an in-flight init() before closing', async () => {
    // When - close lands while init is still bringing the consumer up
    const initPromise = consumer.init()
    const closePromise = consumer.close()
    await Promise.all([initPromise, closePromise])

    // Then - the close applies to the fully built consumer, leaving nothing behind
    expect((consumer as any).consumer).toBeUndefined()
    expect((consumer as any).consumerStream).toBeUndefined()
    expect(consumer.isConnected).toBe(false)
  })
})
