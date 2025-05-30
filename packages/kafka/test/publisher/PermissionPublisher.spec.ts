import { InternalError } from '@lokalise/node-core'
import { type PermissionAdded, TOPICS } from '../utils/permissionSchemas.js'
import { type TestContext, createTestContext } from '../utils/testContext.js'
import { PermissionPublisher } from './PermissionPublisher.js'

describe('PermissionPublisher - init', () => {
  let testContext: TestContext
  let publisher: PermissionPublisher | undefined

  beforeAll(async () => {
    testContext = await createTestContext()
  })

  afterEach(async () => {
    await publisher?.close()
  })

  afterAll(async () => {
    await testContext.dispose()
  })

  describe('init', () => {
    beforeEach(async () => {
      try {
        await testContext.cradle.kafkaAdmin.deleteTopics({
          topics: TOPICS,
        })
      } catch (_) {
        // Ignore errors if the topic does not exist
      }
    })

    it('should thrown an error if topics is empty', () => {
      expect(
        () => new PermissionPublisher(testContext.cradle, { topicsConfig: [] as any }),
      ).toThrowErrorMatchingInlineSnapshot('[Error: At least one topic must be defined]')
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
        autocreateTopics: false,
      })

      // When
      let error: any | undefined
      await publisher.init()
      try {
        await publisher.publish({
          topic: 'permission-general',
          message: {
            id: '1',
            type: 'added',
            permissions: [],
          },
        })
      } catch (e) {
        error = e
      }

      // Then
      expect(error).toBeDefined()
      expect(error).toMatchInlineSnapshot(
        '[InternalError: Error while publishing to Kafka: metadata failed 4 times.]',
      )
      expect(error).toBeInstanceOf(InternalError)
      expect(error.cause).toBeInstanceOf(AggregateError)
      expect(error.cause.errors[0].errors[0].apiId).toBe('UNKNOWN_TOPIC_OR_PARTITION')
    })

    it.each([false, true])(
      'should auto create topic if creation topic is used (lazy init: %s)',
      async (lazyInit) => {
        // Given
        publisher = new PermissionPublisher(testContext.cradle, {
          autocreateTopics: true,
        })

        // When
        if (!lazyInit) await publisher.init()
        await publisher.publish({
          topic: 'permission-general',
          message: {
            id: '1',
            type: 'added',
            permissions: [],
          },
        })

        // Then
        const emittedEvent = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
        expect(emittedEvent.message).toMatchObject({ id: '1', type: 'added' })
      },
    )
  })

  describe('publish', () => {
    it('should fail if there is no schema for message', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      // When
      const message = {
        id: '1',
        type: 'bad' as any, // Intentionally bad type to force the error
        permissions: [],
      } satisfies PermissionAdded
      await expect(
        publisher.publish({
          topic: 'permission-added',
          message,
        }),
      ).rejects.toThrowErrorMatchingInlineSnapshot('[Error: Unsupported message type: bad]')
    })

    it('should fail if message does not match schema', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      // When
      const message = {
        id: 1 as unknown as string,
        type: 'added',
        permissions: [],
      } satisfies PermissionAdded
      await expect(
        publisher.publish({ topic: 'permission-added', message }),
      ).rejects.toThrowErrorMatchingInlineSnapshot(`
        [InternalError: Error while publishing to Kafka: [
          {
            "code": "invalid_type",
            "expected": "string",
            "received": "number",
            "path": [
              "id"
            ],
            "message": "Expected string, received number"
          }
        ]]
      `)
    })
  })
})
