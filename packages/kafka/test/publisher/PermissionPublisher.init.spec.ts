import type { Admin } from '@platformatic/kafka'
import { afterAll } from 'vitest'
import { type TestContext, createTestContext } from '../utils/testContext.js'
import { PermissionPublisher } from './PermissionPublisher.js'

describe('PermissionPublisher - init', () => {
  let testContext: TestContext
  let publisher: PermissionPublisher | undefined
  let kafkaAdmin: Admin

  beforeAll(async () => {
    testContext = await createTestContext()
    kafkaAdmin = testContext.cradle.kafkaAdmin
  })

  beforeEach(async () => {
    try {
      await kafkaAdmin.deleteTopics({
        topics: [PermissionPublisher.TOPIC_NAME],
      })
    } catch (_) {
      // Ignore errors if the topic does not exist
    }
  })

  afterEach(async () => {
    await publisher?.close()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  it('should thrown an error if trying to use spy when it is not enabled', () => {
    // Given
    publisher = new PermissionPublisher(testContext.cradle, { handlerSpy: false })

    // When - Then
    expect(() => publisher?.handlerSpy).toThrowErrorMatchingInlineSnapshot(
      '[Error: HandlerSpy was not instantiated, please pass `handlerSpy` parameter during creation.]',
    )
  })

  it('should fail if topic does not exists', async () => {
    // Given
    publisher = new PermissionPublisher(testContext.cradle, {
      locatorConfig: { topic: PermissionPublisher.TOPIC_NAME },
    })

    // When
    let error: unknown | undefined
    await publisher.init()
    try {
      await publisher.publish({
        id: '1',
        type: 'added',
        permissions: [],
      })
    } catch (e) {
      error = e
    }

    // Then
    expect(error).toBeDefined()
    expect(error).toMatchInlineSnapshot(
      '[InternalError: Error while publishing to Kafka: metadata failed 4 times.]',
    )

    // TODO: error spy?
  })

  it.each([false, true])(
    'should auto create topic if creation topic is used (lazy init: %s)',
    async (lazyInit) => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle, {
        creationConfig: { topic: PermissionPublisher.TOPIC_NAME },
      })

      // When
      if (!lazyInit) await publisher.init()
      await publisher.publish({
        id: '1',
        type: 'added',
        permissions: [],
      })

      // Then
      const emittedEvent = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(emittedEvent.message).toMatchObject({ id: '1', type: 'added' })
    },
  )
})
