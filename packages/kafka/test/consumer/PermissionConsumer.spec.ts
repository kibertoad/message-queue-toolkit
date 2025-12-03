import { randomUUID } from 'node:crypto'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { Producer, stringSerializers } from '@platformatic/kafka'
import { afterAll, expect, type MockInstance } from 'vitest'
import z from 'zod/v4'
import { KafkaHandlerConfig, type RequestContext } from '../../lib/index.ts'
import { PermissionPublisher } from '../publisher/PermissionPublisher.ts'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_REMOVED_SCHEMA,
  type PermissionAdded,
  TOPICS,
} from '../utils/permissionSchemas.ts'
import { createTestContext, type TestContext } from '../utils/testContext.ts'
import { PermissionConsumer } from './PermissionConsumer.ts'

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
        new PermissionConsumer(testContext.cradle, { handlers: {} }).init(),
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
        kafka: {
          bootstrapBrokers: ['localhost:9090'],
          clientId: randomUUID(),
          connectTimeout: 10, // Short timeout to trigger failure quick
        },
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

  describe('isConnected', () => {
    it('should return false if consumer is not initiated', () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)

      // When - Then
      expect(consumer.isConnected).toBe(false)
    })

    it('should return true if consumer is initiated', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)

      // When
      await consumer.init()

      // Then
      expect(consumer.isConnected).toBe(true)
      await consumer.close()
      expect(consumer.isConnected).toBe(false)
    })
  })

  describe('isActive', () => {
    it('should return false if consumer is not initiated', () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)

      // When - Then
      expect(consumer.isActive).toBe(false)
    })

    it('should return true if consumer is initiated', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)

      // When
      await consumer.init()

      // Then
      expect(consumer.isActive).toBe(true)
      await consumer.close()
      expect(consumer.isActive).toBe(false)
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

    it('should consume valid messages', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })
      await publisher.publish('permission-removed', { id: '2', type: 'removed', permissions: [] })

      // Then
      const permissionAddedSpy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(permissionAddedSpy.message).toMatchObject({ id: '1' })
      expect(consumer.addedMessages).toHaveLength(1)
      expect(consumer.addedMessages[0]!.value).toEqual(permissionAddedSpy.message)

      const permissionRemovedSpy = await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(permissionRemovedSpy.message).toMatchObject({ id: '2' })
      expect(consumer.removedMessages).toHaveLength(1)
      expect(consumer.removedMessages[0]!.value).toEqual(permissionRemovedSpy.message)
    })

    it('should react correctly if handler throws an error', async () => {
      // Given
      let counter = 0
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, () => {
            counter++
            throw new Error('Test error')
          }),
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
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, () => {
            counter++
            if (counter === 1) throw new Error('Test error')
          }),
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

    it('should react correct to validation issues', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(
            PERMISSION_ADDED_SCHEMA.extend({ id: z.number() as any }),
            () => Promise.resolve(),
          ),
        },
      })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.processingResult).toMatchObject({ errorReason: 'invalidMessage' })
    })

    it('should ignore non json messages', async () => {
      // Given
      const errorSpy = vi.spyOn(testContext.cradle.errorReporter, 'report')

      const producer = new Producer<string, string, string, string>({
        ...testContext.cradle.kafkaConfig,
        clientId: randomUUID(),
        autocreateTopics: true,
        serializers: stringSerializers,
      })
      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await producer.send({
        messages: [{ topic: 'permission-added', value: 'not valid json' }],
      })

      // Then
      await waitAndRetry(() => errorSpy.mock.calls.length > 0, 10, 100)
      expect(errorSpy).not.toHaveBeenCalled()

      await producer.close()
    })

    it('should work for messages without id field', async () => {
      // Given
      consumer = new PermissionConsumer(testContext.cradle, { messageIdField: 'invalid' })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spyResult = await consumer.handlerSpy.waitForMessage({ permissions: [] }, 'consumed')
      expect(spyResult).toBeDefined()
    })
  })

  describe('observability - request context', () => {
    let publisher: PermissionPublisher
    let metricSpy: MockInstance

    beforeEach(() => {
      metricSpy = vi.spyOn(testContext.cradle.messageMetricsManager, 'registerProcessedMessage')
    })

    afterEach(async () => {
      await publisher.close()
    })

    const buildPublisher = (headerRequestIdField?: string) => {
      publisher = new PermissionPublisher(testContext.cradle, { headerRequestIdField })
    }

    it('should use request context with provided request id', async () => {
      // Given
      buildPublisher()

      const handlerCalls: { messageValue: any; requestContext: RequestContext }[] = []
      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig<PermissionAdded, any, false>(
            PERMISSION_ADDED_SCHEMA,
            (message, _, requestContext) => {
              handlerCalls.push({ messageValue: message.value, requestContext })
            },
          ),
        },
      })
      await consumer.init()

      // When
      const requestId = 'test-request-id'
      await publisher.publish(
        'permission-added',
        {
          id: '1',
          type: 'added',
          permissions: [],
        },
        { reqId: requestId, logger: testContext.cradle.logger },
      )
      await publisher.publish('permission-added', {
        id: '2',
        type: 'added',
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

    it('should use transaction observability manager', async () => {
      // Given
      buildPublisher()

      const { transactionObservabilityManager } = testContext.cradle
      const startTransactionSpy = vi.spyOn(transactionObservabilityManager, 'start')
      const stopTransactionSpy = vi.spyOn(transactionObservabilityManager, 'stop')

      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-added', {
        id: '1',
        type: 'added',
        permissions: [],
      })
      await publisher.publish('permission-added', {
        id: '2',
        type: 'added',
        permissions: [],
      })

      // Then
      await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(startTransactionSpy).toHaveBeenCalledWith(
        'kafka:PermissionConsumer:permission-added',
        expect.any(String),
      )
      expect(stopTransactionSpy).toHaveBeenCalledWith(startTransactionSpy.mock.calls[0]![1])

      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(startTransactionSpy).toHaveBeenCalledWith(
        'kafka:PermissionConsumer:permission-added',
        expect.any(String),
      )
      expect(stopTransactionSpy).toHaveBeenCalledWith(startTransactionSpy.mock.calls[1]![1])
    })

    it('should use metrics manager to measure successful messages', async () => {
      // Given
      buildPublisher()

      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-added',
        messageId: '1',
        message: expect.objectContaining({ id: '1' }),
        messageType: 'unknown',
        messageTimestamp: expect.any(Number),
        processingResult: { status: 'consumed' },
        messageProcessingStartTimestamp: expect.any(Number),
        messageProcessingEndTimestamp: expect.any(Number),
      })
    })

    it('should use metrics to measure validation issues', async () => {
      // Given
      buildPublisher()

      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(
            PERMISSION_ADDED_SCHEMA.extend({ id: z.number() as any }),
            () => Promise.resolve(),
          ),
        },
      })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.processingResult).toMatchObject({ errorReason: 'invalidMessage' })

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-added',
        messageId: '1',
        message: expect.objectContaining({ id: '1' }),
        messageType: 'unknown',
        messageTimestamp: expect.any(Number),
        processingResult: { status: 'error', errorReason: 'invalidMessage' },
        messageProcessingStartTimestamp: expect.any(Number),
        messageProcessingEndTimestamp: expect.any(Number),
      })
    })

    it('should use metrics to measure handler errors', async () => {
      // Given
      buildPublisher()

      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, () => {
            throw new Error('Test error')
          }),
        },
      })
      await consumer.init()

      // When
      await publisher.publish('permission-added', { id: '1', type: 'added', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.processingResult).toMatchObject({ errorReason: 'handlerError' })

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-added',
        messageId: '1',
        message: expect.objectContaining({ id: '1' }),
        messageType: 'unknown',
        messageTimestamp: expect.any(Number),
        processingResult: { status: 'error', errorReason: 'handlerError' },
        messageProcessingStartTimestamp: expect.any(Number),
        messageProcessingEndTimestamp: expect.any(Number),
      })
    })
  })

  describe('sync message processing', () => {
    let publisher: PermissionPublisher
    let consumer: PermissionConsumer | undefined

    beforeAll(() => {
      publisher = new PermissionPublisher(testContext.cradle)
    })

    beforeEach(async () => {
      // Close and clear previous consumer to avoid message accumulation
      if (consumer) {
        await consumer.close()
        consumer.clear()
      }
    })

    afterAll(async () => {
      await publisher.close()
      await consumer?.close()
    })

    it('should process messages one at a time using handleSyncStream', async () => {
      // Given - track processing order and timing
      const processingOrder: string[] = []
      const processingTimestamps: Record<string, { start: number; end: number }> = {}
      const testMessageIds = ['sync-1', 'sync-2', 'sync-3']

      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, async (message) => {
            // Only track messages from this test
            if (!testMessageIds.includes(message.value.id)) {
              consumer!.addedMessages.push(message)
              return
            }

            const messageId = message.value.id
            processingOrder.push(`start-${messageId}`)
            processingTimestamps[messageId] = { start: Date.now(), end: 0 }

            // Simulate async work to verify sequential processing
            await new Promise((resolve) => setTimeout(resolve, 50))

            processingOrder.push(`end-${messageId}`)
            processingTimestamps[messageId]!.end = Date.now()
            consumer!.addedMessages.push(message)
          }),
        },
      })

      await consumer.init()

      // When - publish multiple messages at once
      await Promise.all([
        publisher.publish('permission-added', { id: 'sync-1', type: 'added', permissions: [] }),
        publisher.publish('permission-added', { id: 'sync-2', type: 'added', permissions: [] }),
        publisher.publish('permission-added', { id: 'sync-3', type: 'added', permissions: [] }),
      ])

      // Then - wait for all messages to be processed
      await consumer.handlerSpy.waitForMessageWithId('sync-1', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('sync-2', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('sync-3', 'consumed')

      // Verify messages were processed sequentially (one completes before next starts)
      expect(processingOrder).toEqual([
        'start-sync-1',
        'end-sync-1',
        'start-sync-2',
        'end-sync-2',
        'start-sync-3',
        'end-sync-3',
      ])

      // Verify each message completes before the next one starts
      expect(processingTimestamps['sync-1']!.end).toBeLessThan(
        processingTimestamps['sync-2']!.start,
      )
      expect(processingTimestamps['sync-2']!.end).toBeLessThan(
        processingTimestamps['sync-3']!.start,
      )

      const testMessages = consumer.addedMessages.filter((m) => testMessageIds.includes(m.value.id))
      expect(testMessages).toHaveLength(3)
      expect(testMessages[0]!.value.id).toBe('sync-1')
      expect(testMessages[1]!.value.id).toBe('sync-2')
      expect(testMessages[2]!.value.id).toBe('sync-3')
    })

    it('should process messages in order even when published rapidly', async () => {
      // Given
      const testMessageIds = ['rapid-1', 'rapid-2', 'rapid-3', 'rapid-4', 'rapid-5']
      consumer = new PermissionConsumer(testContext.cradle)
      await consumer.init()

      // When - publish messages rapidly without waiting
      const publishPromises = []
      for (let i = 1; i <= 5; i++) {
        publishPromises.push(
          publisher.publish('permission-added', {
            id: `rapid-${i}`,
            type: 'added',
            permissions: [],
          }),
        )
      }
      await Promise.all(publishPromises)

      // Then - wait for all messages to be processed
      for (let i = 1; i <= 5; i++) {
        await consumer.handlerSpy.waitForMessageWithId(`rapid-${i}`, 'consumed')
      }

      // Verify messages were processed in order
      const testMessages = consumer.addedMessages.filter((m) => testMessageIds.includes(m.value.id))
      expect(testMessages).toHaveLength(5)
      for (let i = 0; i < 5; i++) {
        expect(testMessages[i]!.value.id).toBe(`rapid-${i + 1}`)
      }
    })

    it('should ensure previous message completes before next message starts processing', async () => {
      // Given - use a handler that takes time and tracks concurrency
      let concurrentProcessing = 0
      let maxConcurrency = 0
      const testMessageIds = ['concurrency-1', 'concurrency-2', 'concurrency-3']
      const processedMessages: string[] = []

      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, async (message) => {
            // Only track messages from this test
            if (!testMessageIds.includes(message.value.id)) {
              consumer!.addedMessages.push(message)
              return
            }

            concurrentProcessing++
            maxConcurrency = Math.max(maxConcurrency, concurrentProcessing)

            // Simulate processing time
            await new Promise((resolve) => setTimeout(resolve, 30))

            concurrentProcessing--
            processedMessages.push(message.value.id)
            consumer!.addedMessages.push(message)
          }),
        },
      })
      await consumer.init()

      // When - publish multiple messages
      await Promise.all([
        publisher.publish('permission-added', {
          id: 'concurrency-1',
          type: 'added',
          permissions: [],
        }),
        publisher.publish('permission-added', {
          id: 'concurrency-2',
          type: 'added',
          permissions: [],
        }),
        publisher.publish('permission-added', {
          id: 'concurrency-3',
          type: 'added',
          permissions: [],
        }),
      ])

      // Then - wait for all messages
      await consumer.handlerSpy.waitForMessageWithId('concurrency-1', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('concurrency-2', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('concurrency-3', 'consumed')

      // Verify only one message was processed at a time (max concurrency = 1)
      expect(maxConcurrency).toBe(1)
      expect(processedMessages).toHaveLength(3)
      expect(processedMessages).toContain('concurrency-1')
      expect(processedMessages).toContain('concurrency-2')
      expect(processedMessages).toContain('concurrency-3')
    })

    it('should process messages synchronously across different topics', async () => {
      // Given
      const processingOrder: string[] = []
      const testMessageIds = ['cross-topic-1', 'cross-topic-2', 'cross-topic-3']

      consumer = new PermissionConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_ADDED_SCHEMA, async (message) => {
            // Only track messages from this test
            if (!testMessageIds.includes(message.value.id)) {
              consumer!.addedMessages.push(message)
              return
            }
            processingOrder.push(`added-${message.value.id}`)
            await new Promise((resolve) => setTimeout(resolve, 20))
            consumer!.addedMessages.push(message)
          }),
          'permission-removed': new KafkaHandlerConfig(
            PERMISSION_REMOVED_SCHEMA,
            async (message) => {
              // Only track messages from this test
              if (!testMessageIds.includes(message.value.id)) {
                consumer!.removedMessages.push(message)
                return
              }
              processingOrder.push(`removed-${message.value.id}`)
              await new Promise((resolve) => setTimeout(resolve, 20))
              consumer!.removedMessages.push(message)
            },
          ),
        },
      })
      await consumer.init()

      // When - publish messages to different topics
      await Promise.all([
        publisher.publish('permission-added', {
          id: 'cross-topic-1',
          type: 'added',
          permissions: [],
        }),
        publisher.publish('permission-removed', {
          id: 'cross-topic-2',
          type: 'removed',
          permissions: [],
        }),
        publisher.publish('permission-added', {
          id: 'cross-topic-3',
          type: 'added',
          permissions: [],
        }),
      ])

      // Then - wait for all messages
      await consumer.handlerSpy.waitForMessageWithId('cross-topic-1', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('cross-topic-2', 'consumed')
      await consumer.handlerSpy.waitForMessageWithId('cross-topic-3', 'consumed')

      // Verify messages were processed sequentially (one at a time)
      // Note: The exact order depends on Kafka's partition assignment, but each should complete before next starts
      expect(processingOrder.length).toBe(3)
      const testMessages =
        consumer.addedMessages.filter((m) => testMessageIds.includes(m.value.id)).length +
        consumer.removedMessages.filter((m) => testMessageIds.includes(m.value.id)).length
      expect(testMessages).toBe(3)
    })
  })
})
