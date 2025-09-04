import { randomUUID } from 'node:crypto'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { Producer, stringSerializers } from '@platformatic/kafka'
import { afterAll, expect, type MockInstance } from 'vitest'
import z from 'zod/v4'
import {
  KafkaBatchHandlerConfig,
  KafkaHandlerConfig,
  type RequestContext,
} from '../../lib/index.ts'
import { PermissionPublisher } from '../publisher/PermissionPublisher.ts'
import {
  PERMISSION_ADDED_SCHEMA,
  PERMISSION_SCHEMA,
  type PermissionAdded,
  TOPICS,
} from '../utils/permissionSchemas.ts'
import { createTestContext, type TestContext } from '../utils/testContext.ts'
import { PermissionBatchConsumer } from './PermissionBatchConsumer.ts'

describe('PermissionBatchConsumer', () => {
  let testContext: TestContext
  let consumer: PermissionBatchConsumer | undefined

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
        new PermissionBatchConsumer(testContext.cradle, { handlers: {} }).init(),
      ).rejects.toThrowErrorMatchingInlineSnapshot('[Error: At least one topic must be defined]')
    })

    it('should thrown an error if trying to use spy when it is not enabled', () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle, { handlerSpy: false })

      // When - Then
      expect(() => consumer?.handlerSpy).toThrowErrorMatchingInlineSnapshot(
        '[Error: HandlerSpy was not instantiated, please pass `handlerSpy` parameter during creation.]',
      )
    })

    it('should not fail on close if consumer is not initiated', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle, { handlers: {} })
      // When - Then
      await expect(consumer.close()).resolves.not.toThrowError()
    })

    it('should not fail on init if it is already initiated', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle)
      // When
      await consumer.init()

      // Then
      await expect(consumer.init()).resolves.not.toThrowError()
    })

    it('should fail if kafka is not available', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle, {
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
      consumer = new PermissionBatchConsumer(testContext.cradle, { autocreateTopics: false })

      // When - Then
      await expect(consumer.init()).rejects.toThrowErrorMatchingInlineSnapshot(
        '[InternalError: Consumer init failed]',
      )
    })

    it('should work if topic does not exists and autocreate is enabled', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle)

      // When - Then
      await expect(consumer.init()).resolves.not.toThrowError()
    })
  })

  describe('isConnected', () => {
    it('should return false if consumer is not initiated', () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle)

      // When - Then
      expect(consumer.isConnected).toBe(false)
    })

    it('should return true if consumer is initiated', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle)

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
      consumer = new PermissionBatchConsumer(testContext.cradle)

      // When - Then
      expect(consumer.isActive).toBe(false)
    })

    it('should return true if consumer is initiated', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle)

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

    it('should consume valid messages when batch size is reached', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        batchProcessingOptions: {
          batchSize: 2,
          timeoutMilliseconds: 5000,
        },
      })
      await consumer.init()

      const messages: PermissionAdded[] = [
        { id: '1', type: 'added', permissions: [] },
        { id: '2', type: 'added', permissions: [] },
        { id: '3', type: 'added', permissions: [] },
      ]

      // When
      for (const message of messages) {
        await publisher.publish('permission-added', message)
      }

      // Then
      await Promise.all([
        consumer.handlerSpy.waitForMessageWithId('1', 'consumed'),
        consumer.handlerSpy.waitForMessageWithId('2', 'consumed'),
      ])
      // Only 1 batch was delivered
      expect(consumer.addedMessages).toHaveLength(1)
      // Batch contains 2 messages
      expect(consumer.addedMessages[0]).toHaveLength(2)
      // Messages are in order
      expect(consumer.addedMessages[0]!.map((m) => m.value)).toEqual([messages[0], messages[1]])
    })

    it('should consume valid messages when batch timeout is reached', async () => {
      // Given
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        batchProcessingOptions: {
          batchSize: 200,
          timeoutMilliseconds: 1000, // 1s timeout to trigger batch processing
        },
      })
      await consumer.init()

      const messages: PermissionAdded[] = [
        { id: '1', type: 'added', permissions: [] },
        { id: '2', type: 'added', permissions: [] },
        { id: '3', type: 'added', permissions: [] },
      ]

      // When
      for (const message of messages) {
        await publisher.publish('permission-added', message)
      }

      // Then
      await Promise.all([
        consumer.handlerSpy.waitForMessageWithId('1', 'consumed'),
        consumer.handlerSpy.waitForMessageWithId('2', 'consumed'),
        consumer.handlerSpy.waitForMessageWithId('3', 'consumed'),
      ])
      // Only 1 batch was delivered
      expect(consumer.addedMessages).toHaveLength(1)
      // Batch contains 2 messages
      expect(consumer.addedMessages[0]).toHaveLength(3)
      // Messages are in order
      expect(consumer.addedMessages[0]!.map((m) => m.value)).toEqual(messages)
    })

    it('should react correctly if handler throws an error', async () => {
      // Given
      let counter = 0
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_SCHEMA, () => {
            counter++
            throw new Error('Test error')
          }),
        },
        batchProcessingOptions: {
          batchSize: 1, // Single message batch to trigger handler
          timeoutMilliseconds: 10000,
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
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        handlers: {
          'permission-added': new KafkaHandlerConfig(PERMISSION_SCHEMA, () => {
            counter++
            if (counter === 1) throw new Error('Test error')
          }),
        },
        batchProcessingOptions: {
          batchSize: 1, // Single message batch to trigger handler
          timeoutMilliseconds: 10000,
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
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_SCHEMA.extend({ id: z.number() }), () =>
              Promise.resolve(),
            ),
          ],
        },
        batchProcessingOptions: {
          batchSize: 1, // Single message batch to trigger handler
          timeoutMilliseconds: 10000,
        },
      } as any)
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', permissions: [] })

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
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        batchProcessingOptions: {
          batchSize: 1, // Single message batch to trigger handler
          timeoutMilliseconds: 10000,
        },
      })
      await consumer.init()

      // When
      await producer.send({
        messages: [{ topic: 'permission-general', value: 'not valid json' }],
      })

      // Then
      await waitAndRetry(() => errorSpy.mock.calls.length > 0, 10, 100)
      expect(errorSpy).not.toHaveBeenCalled()

      await producer.close()
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

      const handlerCalls: { messages: any[]; requestContext: RequestContext }[] = []
      consumer = new PermissionBatchConsumer(testContext.cradle, {
        handlers: {
          'permission-added': [
            new KafkaBatchHandlerConfig(PERMISSION_ADDED_SCHEMA, (messages, _, requestContext) => {
              handlerCalls.push({ messages: messages.map((m) => m.value), requestContext })
            }),
          ],
        },
        batchProcessingOptions: {
          batchSize: 1,
          timeoutMilliseconds: 100,
        },
      } as any)
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

      // Then
      const spy1 = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy1.message).toMatchObject({ id: '1' })
      expect(handlerCalls[0]!.messages[0]).toEqual(spy1.message)
      expect(handlerCalls[0]!.requestContext).toMatchObject({ reqId: requestId })
    })

    it('should use transaction observability manager', async () => {
      // Given
      buildPublisher()

      const { transactionObservabilityManager } = testContext.cradle
      const startTransactionSpy = vi.spyOn(transactionObservabilityManager, 'start')
      const stopTransactionSpy = vi.spyOn(transactionObservabilityManager, 'stop')

      consumer = new PermissionBatchConsumer(testContext.cradle, {
        batchProcessingOptions: {
          batchSize: 1,
          timeoutMilliseconds: 100,
        },
      })
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
        'kafka:PermissionBatchConsumer:permission-added:batch',
        expect.any(String),
      )
      const firstTransactionId = startTransactionSpy.mock.calls[0]![1].toString()
      expect(stopTransactionSpy).toHaveBeenCalledWith(firstTransactionId)

      await consumer.handlerSpy.waitForMessageWithId('2', 'consumed')
      expect(startTransactionSpy).toHaveBeenCalledWith(
        'kafka:PermissionBatchConsumer:permission-added:batch',
        expect.any(String),
      )
      const secondTransactionId = startTransactionSpy.mock.calls[0]![1]
      expect(stopTransactionSpy).toHaveBeenCalledWith(secondTransactionId)
    })

    it('should use metrics manager to measure successful messages', async () => {
      // Given
      buildPublisher()

      consumer = new PermissionBatchConsumer(testContext.cradle)
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'consumed')
      expect(spy.message).toMatchObject({ id: '1' })

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-general',
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

      consumer = new PermissionBatchConsumer(testContext.cradle, {
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

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-general',
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

      consumer = new PermissionBatchConsumer(testContext.cradle, {
        handlers: {
          'permission-general': [
            new KafkaHandlerConfig(PERMISSION_SCHEMA, () => {
              throw new Error('Test error')
            }),
          ],
        },
      } as any)
      await consumer.init()

      // When
      await publisher.publish('permission-general', { id: '1', permissions: [] })

      // Then
      const spy = await consumer.handlerSpy.waitForMessageWithId('1', 'error')
      expect(spy.processingResult).toMatchObject({ errorReason: 'handlerError' })

      expect(metricSpy).toHaveBeenCalledTimes(2) // publish + consume
      expect(metricSpy).toHaveBeenCalledWith({
        queueName: 'permission-general',
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
})
