import { randomUUID } from 'node:crypto'
import { TOPICS } from '../utils/permissionSchemas.js'
import { type TestContext, createTestContext } from '../utils/testContext.js'
import { PermissionConsumer } from './PermissionConsumer.js'

describe('PermissionConsumer', () => {
  let testContext: TestContext
  let consumer: PermissionConsumer | undefined

  beforeAll(async () => {
    testContext = await createTestContext()
  })

  afterEach(async () => {
    await consumer?.close()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  describe('init - close', () => {
    beforeEach(async () => {
      try {
        await testContext.cradle.kafkaAdmin.deleteTopics({
          topics: TOPICS,
        })
      } catch (_) {
        // Ignore errors if the topic does not exist
      }
    })

    it('should thrown an error if topics is empty', async () => {
      await expect(
        new PermissionConsumer(testContext.cradle, { handlers: {} }).init(),
      ).rejects.toThrowErrorMatchingInlineSnapshot('[Error: At least one topic must be defined]')

      await expect(
        new PermissionConsumer(testContext.cradle, { handlers: { test: [] } }).init(),
      ).rejects.toThrowErrorMatchingInlineSnapshot('[Error: At least one topic must be defined]')
    })

    it('should thrown an error if trying to use spy when it is not enabled', () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, { handlerSpy: false })

      // When - Then
      expect(() => consumer?.handlerSpy).toThrowErrorMatchingInlineSnapshot(
        '[Error: HandlerSpy was not instantiated, please pass `handlerSpy` parameter during creation.]',
      )
    })

    it('should not fail on close if consumer is not initiated', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, { handlers: {} })
      // When - Then
      await expect(consumer.close()).resolves.not.toThrowError()
    })

    it('should fail if kafka is not available', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, {
        kafka: { clientId: randomUUID(), bootstrapBrokers: ['localhost:9091'] },
        connectTimeout: 10, // Short timeout to trigger failure quick
      })

      // When - Then
      await expect(consumer.init()).rejects.toThrowErrorMatchingInlineSnapshot(
        '[InternalError: Consumer init failed]',
      )
    })

    it('should fail if kafka is not available', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, {
        kafka: { clientId: randomUUID(), bootstrapBrokers: ['localhost:9091'] },
        connectTimeout: 10, // Short timeout to trigger failure quick
      })

      // When - Then
      await expect(consumer.init()).rejects.toThrowErrorMatchingInlineSnapshot(
        '[InternalError: Consumer init failed]',
      )
    })

    it('should fail if topic does not exists and autocreate is disabled', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, { autocreateTopics: false })

      // When - Then
      await expect(consumer.init()).rejects.toThrowErrorMatchingInlineSnapshot(
        '[InternalError: Consumer init failed]',
      )
    })

    it('should work if topic does not exists and autocreate is enabled', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)

      // When - Then
      await expect(consumer.init()).resolves.not.toThrowError()
    })
  })
})
