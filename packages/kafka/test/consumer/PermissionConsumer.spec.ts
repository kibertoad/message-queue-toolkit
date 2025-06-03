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

    it('should not fail on close if consumer is not initiated', async () => {
      consumer = new PermissionConsumer(testContext.cradle, { handlers: {} })
      await expect(consumer.close()).resolves.not.toThrowError()
    })

    // TODO
    it('should fail if kafka is not available', async () => {
      consumer = new PermissionConsumer(testContext.cradle, {
        kafka: { clientId: randomUUID(), bootstrapBrokers: ['test.com'] },
      })
      await consumer.init()
    })
  })
})
