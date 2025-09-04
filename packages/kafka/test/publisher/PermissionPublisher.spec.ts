import { randomUUID } from 'node:crypto'
import { InternalError } from '@lokalise/node-core'
import type { MockInstance } from 'vitest'
import z from 'zod/v4'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_ADDED_TOPIC,
  type PermissionAdded,
  type PermissionRemoved,
  TOPICS,
} from '../utils/permissionSchemas.ts'
import { createTestContext, type TestContext } from '../utils/testContext.ts'
import { PermissionPublisher } from './PermissionPublisher.ts'

describe('PermissionPublisher', () => {
  let testContext: TestContext
  let publisher: PermissionPublisher

  beforeAll(async () => {
    testContext = await createTestContext()
  })

  afterEach(async () => {
    await publisher?.close()
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

    it('should fail if kafka is not available', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle, {
        // port 9090 is not valid
        kafka: { bootstrapBrokers: ['localhost:9090'], clientId: randomUUID() },
      })

      // When - Then
      await expect(publisher.init()).rejects.toThrowErrorMatchingInlineSnapshot(
        '[InternalError: Producer init failed]',
      )
    })

    it('should not fail on close if publisher is not started ', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      // When - Then
      await expect(publisher.close()).resolves.not.toThrow()
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
        await publisher.publish('permission-added', {
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
        await publisher.publish('permission-added', {
          id: '1',
          type: 'added',
          permissions: [],
        })

        // Then
        const emittedEvent = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
        expect(emittedEvent.message).toMatchObject({ id: '1', type: 'added' })
      },
    )

    it('should fail if a message has more than one schema', () => {
      expect(
        () =>
          new PermissionPublisher(testContext.cradle, {
            topicsConfig: [
              {
                topic: 'permission-added',
                schemas: [PERMISSION_ADDED_SCHEMA, PERMISSION_ADDED_SCHEMA],
              },
            ] as any,
          }),
      ).toThrowErrorMatchingInlineSnapshot(
        '[Error: Duplicate schema for type: Symbol(NO_MESSAGE_TYPE)]',
      )

      expect(
        () =>
          new PermissionPublisher(testContext.cradle, {
            topicsConfig: [
              {
                topic: 'permission-added',
                schemas: [z.object({}), z.object({})],
              },
            ] as any,
          }),
      ).toThrowErrorMatchingInlineSnapshot(
        '[Error: Duplicate schema for type: Symbol(NO_MESSAGE_TYPE)]',
      )
    })
  })

  describe('publish', () => {
    let metricsSpy: MockInstance

    beforeEach(() => {
      metricsSpy = vi.spyOn(testContext.cradle.messageMetricsManager, 'registerProcessedMessage')
    })

    it('should fail if topic is not supported', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      // When
      await expect(
        publisher.publish('bad topic' as any, {} as any), // Intentionally bad topic to force the error
      ).rejects.toThrowErrorMatchingInlineSnapshot(
        '[Error: Message schemas not found for topic: bad topic]',
      )
    })

    it('should fail if there is no schema for message type', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      const message = {
        id: '1',
        type: 'bad' as any, // Intentionally bad type to force the error
        permissions: [],
      } satisfies PermissionAdded

      // When
      await expect(
        publisher.publish('permission-added', message),
      ).rejects.toThrowErrorMatchingInlineSnapshot(`
        [InternalError: Error while publishing to Kafka: [
          {
            "code": "invalid_value",
            "values": [
              "added"
            ],
            "path": [
              "type"
            ],
            "message": "Invalid input: expected \\"added\\""
          }
        ]]
      `)
    })

    it('should fail if message does not match schema', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      const message = {
        id: 1 as unknown as string,
        type: 'added',
        permissions: [],
      } satisfies PermissionAdded

      // When
      await expect(
        publisher.publish('permission-added', message),
      ).rejects.toThrowErrorMatchingInlineSnapshot(`
        [InternalError: Error while publishing to Kafka: [
          {
            "expected": "string",
            "code": "invalid_type",
            "path": [
              "id"
            ],
            "message": "Invalid input: expected string, received number"
          }
        ]]
      `)
    })

    it('should publish messages', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle)

      const message1 = {
        id: '1',
        type: 'added',
        permissions: [],
      } satisfies PermissionAdded
      const message2 = {
        id: '2',
        type: 'removed',
        permissions: [],
      } satisfies PermissionRemoved

      // When
      await publisher.publish('permission-added', message1)
      await publisher.publish('permission-removed', message2)

      // Then
      const emittedEvent1 = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(emittedEvent1.message).toMatchObject(message1)

      const emittedEvent2 = await publisher.handlerSpy.waitForMessageWithId('2', 'published')
      expect(emittedEvent2.message).toMatchObject(message2)

      expect(metricsSpy).toHaveBeenCalledTimes(2)
      expect(metricsSpy).toHaveBeenNthCalledWith(1, {
        queueName: 'permission-added',
        messageId: '1',
        message: message1,
        messageType: 'unknown',
        messageTimestamp: undefined,
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp: expect.any(Number),
        messageProcessingEndTimestamp: expect.any(Number),
      })
      expect(metricsSpy).toHaveBeenNthCalledWith(2, {
        queueName: 'permission-removed',
        messageId: '2',
        message: message2,
        messageType: 'unknown',
        messageTimestamp: undefined,
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp: expect.any(Number),
        messageProcessingEndTimestamp: expect.any(Number),
      })
    })

    it('should publish only messages meeting schema', async () => {
      // Given
      publisher = new PermissionPublisher(testContext.cradle, {
        topicsConfig: [
          {
            topic: PERMISSION_ADDED_TOPIC,
            schemas: [PERMISSION_ADDED_SCHEMA],
          },
        ] as any, // we are not adding the other topics intentionally
      })

      const messageValid = {
        id: '1',
        type: 'added',
        permissions: [],
      } satisfies PermissionAdded
      const messageInvalid = {
        id: 2 as any,
        type: 'added',
        permissions: [],
      } satisfies PermissionAdded

      // When&Then - valid
      await publisher.publish(PERMISSION_ADDED_TOPIC, messageValid)
      const emittedEvent = await publisher.handlerSpy.waitForMessageWithId('1', 'published')
      expect(emittedEvent.message).toMatchObject(messageValid)

      // When&Then - invalid
      await expect(
        publisher.publish(PERMISSION_ADDED_TOPIC, messageInvalid),
      ).rejects.toThrowErrorMatchingInlineSnapshot(`
        [InternalError: Error while publishing to Kafka: [
          {
            "expected": "string",
            "code": "invalid_type",
            "path": [
              "id"
            ],
            "message": "Invalid input: expected string, received number"
          }
        ]]
      `)
    })
  })
})
