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

  it('should call close and init again after stream error', async () => {
    // Given
    await consumer.init()

    const closeSpy = vi.spyOn(consumer, 'close')
    const initSpy = vi.spyOn(consumer, 'init')

    // When
    simulateStreamError()

    // Then - reconnect should trigger close and init
    await waitAndRetry(() => closeSpy.mock.calls.length === 1)
    expect(consumer.isActive).toBe(false)
    await waitAndRetry(() => initSpy.mock.calls.length === 1, 100, 15)

    expect(closeSpy).toHaveBeenCalledTimes(1)
    expect(initSpy).toHaveBeenCalledTimes(1)
    expect(consumer.isActive).toBe(true)
  })
})
