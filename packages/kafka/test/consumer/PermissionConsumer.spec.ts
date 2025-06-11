import { randomUUID } from 'node:crypto'
import { afterAll, expect } from 'vitest'
import z from 'zod/v3'
import { KafkaHandlerConfig, type RequestContext } from '../../lib/index.js'
import { PermissionPublisher } from '../publisher/PermissionPublisher.js'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_REMOVED_SCHEMA,
  PERMISSION_SCHEMA,
  TOPICS,
} from '../utils/permissionSchemas.js'
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

    it('should not fail on init if it is already initiated', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)
      // When
      await consumer.init()

      // Then
      await expect(consumer.init()).resolves.not.toThrowError()
    })

    it('should fail if kafka is not available', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, {
        // port 9090 is not valid
        kafka: { bootstrapBrokers: ['localhost:9090'], clientId: randomUUID() },
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

  describe('consume', () => {
    let publisher: PermissionPublisher

    beforeAll(() => {
      publisher = new PermissionPublisher(testContext.cradle)
    })

    afterAll(async () => {
      await publisher.close()
    })

    it('should consume messages with valid type', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })
      expect(consumer.addedMessages).toHaveLength(1)
      expect(consumer.addedMessages[0]!.value).toEqual(spy.message)
    })

    it('should consume messages without type with default handler', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })
      expect(consumer.noTypeMessages).toHaveLength(1)
      expect(consumer.noTypeMessages[0]!.value).toEqual(spy.message)
    })

    it('should react correctly if handler throws an error', async () => {
      // Given
      let counter = 0
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': [
            new KafkaHandlerConfig(PERMISSION_SCHEMA, () => {
              counter++
              throw new Error('Test error')
            }),
          ],
        },
      })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.message).toMatchObject({ id: '1' })
      expect(counter).toBe(3)
    })

    it('should consume message after initial error', async () => {
      // Given
      let counter = 0
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': [
            new KafkaHandlerConfig(PERMISSION_SCHEMA, () => {
              counter++
              if (counter === 1) throw new Error('Test error')
            }),
          ],
        },
      })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })
      expect(counter).toBe(2)
    })

    it('should ignore event if handler does not exists', async () => {
      // Given
      let removeCounter = 0
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_REMOVED_SCHEMA, () => {
              removeCounter++
            }),
          ],
        } as any,
      })
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', type: 'added', permissions: [] })
      await publisher.publish('permission-general', { id: '2', type: 'removed', permissions: [] })

      // Then
      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(removeCounter).toBe(1)
    })

    it('should react correct to validation issues', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_SCHEMA.extend({ id: z.number() }), () =>
              Promise.resolve(),
            ),
          ],
        },
      } as any)
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.processingResult).toMatchObject({ errorReason: 'invalidMessage' })
    })
  })

  describe('observability - request context', () => {
    let publisher: PermissionPublisher

    afterEach(async () => {
      await publisher.close()
    })

    const buildPublisher = (headerRequestIdField?: string) => {
      publisher = new PermissionPublisher(testContext.cradle, { headerRequestIdField })
    }

    it('should use transaction observability manager', async () => {
      // Given
      buildPublisher()

      const { transactionObservabilityManager } = testContext.cradle
      const startTransactionSpy = vi.spyOn(transactionObservabilityManager, 'start')
      const stopTransactionSpy = vi.spyOn(transactionObservabilityManager, 'stop')

      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-general', {
        id: '1',
        type: 'added',
        permissions: [],
      })
      await publisher.publish('permission-general', {
        id: '2',
        permissions: [],
      })

      // Then
      await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(startTransactionSpy).toHaveBeenCalledWith('kafka:permission-general:added', '1')
      expect(stopTransactionSpy).toHaveBeenCalledWith('1')

      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(startTransactionSpy).toHaveBeenCalledWith('kafka:permission-general', '2')
      expect(stopTransactionSpy).toHaveBeenCalledWith('2')
    })

    it('should use request context with default request id from headers', async () => {
      // Given
      buildPublisher()

      const handlerCalls: { messageValue: any; requestContext: RequestContext }[] = []
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (message, requestContext) => {
              handlerCalls.push({ messageValue: message.value, requestContext })
            }),
            new KafkaHandlerConfig(PERMISSION_SCHEMA, (message, requestContext) => {
              handlerCalls.push({ messageValue: message.value, requestContext })
            }),
          ],
        },
      } as any)
      await consumer.init()

      // When
      const requestId = 'test-request-id'
      await publisher.publish(
        'permission-general',
        {
          id: '1',
          type: 'added',
          permissions: [],
        },
        { reqId: requestId, logger: testContext.cradle.logger },
      )
      await publisher.publish('permission-general', {
        id: '2',
        permissions: [],
      })

      // Then
      const spy1 = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy1.message).toMatchObject({ id: '1' })
      expect(spy1.message).toEqual(handlerCalls[0]!.messageValue)
      expect(handlerCalls[0]!.requestContext).toMatchObject({ reqId: requestId })

      const spy2 = await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(spy2.message).toMatchObject({ id: '2' })
      expect(spy2.message).toEqual(handlerCalls[1]!.messageValue)
      expect(handlerCalls[1]!.requestContext).not.toMatchObject({ reqId: requestId })
    })

    it('should use request context with default request id from headers', async () => {
      // Given
      const headerRequestIdField = 'my-field'
      buildPublisher(headerRequestIdField)

      const handlerCalls: { messageValue: any; requestContext: RequestContext }[] = []
      consumer = new PermissionConsumer(testContext.cradle, {
        headerRequestIdField,
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, (message, requestContext) => {
              handlerCalls.push({ messageValue: message.value, requestContext })
            }),
          ],
        },
      } as any)
      await consumer.init()

      // When
      const requestId = 'test-request-id'
      await publisher.publish(
        'permission-general',
        {
          id: '1',
          type: 'added',
          permissions: [],
        },
        { reqId: requestId, logger: testContext.cradle.logger },
        { headers: { 'x-request-id': 'another-id' } },
      )

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })
      expect(spy.message).toEqual(handlerCalls[0]!.messageValue)
      expect(handlerCalls[0]!.requestContext).toMatchObject({ reqId: requestId })
    })
  })
})
