import { AbstractKafkaConsumer } from './AbstractKafkaConsumer.ts'

describe('AbstractKafkaConsumer - handleSyncStreamBatch', () => {
  it('must call kafkaStream.pause before and kafkaStream.resume after consuming messages', async () => {
    // Given
    const logger = {
      debug: vi.fn(),
      error: vi.fn(),
      child: vi.fn(() => logger),
    }

    const dependencies = {
      logger,
      errorReporter: { report: vi.fn() },
      messageMetricsManager: undefined,
      transactionObservabilityManager: {
        start: vi.fn(),
        stop: vi.fn(),
      },
    } as any

    const options = {
      kafka: { bootstrapBrokers: ['localhost:9092'], clientId: 'test-client' },
      groupId: 'test-group',
      handlers: {},
      batchProcessingEnabled: true,
      batchProcessingOptions: {
        batchSize: 10,
        timeoutMilliseconds: 1_000,
      },
    } as any

    const executionContext = {}

    // Bypass abstract/visibility checks with `as any`
    const consumer = new (AbstractKafkaConsumer as any)(
      dependencies,
      options,
      executionContext,
    ) as any

    const calls: string[] = []

    consumer.consume = vi.fn(() => {
      calls.push('consume')
    })

    const kafkaStream = {
      pause: vi.fn(() => {
        calls.push('pause')
      }),
      resume: vi.fn(() => {
        calls.push('resume')
      }),
    } as any

    // Attach mocked stream so handleSyncStreamBatch uses it via this.consumerStream
    ;(consumer as any).consumerStream = kafkaStream

    const messageBatch = {
      topic: 'test-topic',
      partition: 0,
      messages: [{ value: { foo: 'bar' } }],
    }

    const batchStream: any = {
      async *[Symbol.asyncIterator]() {
        // Add an await to satisfy async function lint rule
        await Promise.resolve()
        yield messageBatch
      },
    }

    // When
    await (consumer as any).handleSyncStreamBatch(batchStream)

    // Then
    expect(calls).toEqual(['pause', 'consume', 'resume'])
    expect(kafkaStream.pause).toHaveBeenCalledTimes(1)
    expect(kafkaStream.resume).toHaveBeenCalledTimes(1)
    expect(consumer.consume).toHaveBeenCalledTimes(1)
  })
})
