import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Counter } from 'prom-client'
import * as promClient from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageByStatusCounter } from './PrometheusMessageByStatusCounter.ts'

type TestMessage = {
  id: string
  messageType: 'test'
  metadata?: {
    schemaVersion: string
  }
}

const mockCounterCalls = () => {
  const counterCalls: { labels: Record<string, unknown>; value?: number }[] = []
  vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
    inc(labels: Record<string, string | number>, value?: number) {
      counterCalls.push({ labels, value })
    },
  } as Counter)
  return counterCalls
}

const buildMetadata = (
  message: TestMessage,
  overrides?: Partial<ProcessedMessageMetadata<TestMessage>>,
): ProcessedMessageMetadata<TestMessage> => ({
  messageId: message.id,
  messageType: message.messageType,
  processingResult: { status: 'consumed' },
  message,
  queueName: 'test-queue',
  messageTimestamp: Date.now(),
  messageProcessingStartTimestamp: Date.now(),
  messageProcessingEndTimestamp: Date.now(),
  ...overrides,
})

describe('PrometheusMessageByStatusCounter', () => {
  it.each([
    { status: 'consumed' },
    { status: 'published' },
    { status: 'retryLater' },
    { status: 'error', errorReason: 'handlerError' },
  ] as const)('registers resultStatus label for %o', (processingResult) => {
    // Given
    const counterCalls = mockCounterCalls()
    const metric = new PrometheusMessageByStatusCounter<TestMessage>(
      { name: 'test_metric', helpDescription: 'test', labelNames: ['resultStatus'] },
      promClient,
    )

    // When
    metric.registerProcessedMessage(
      buildMetadata({ id: '1', messageType: 'test' }, { processingResult }),
    )

    // Then
    expect(counterCalls).toHaveLength(1)
    expect(counterCalls[0]?.labels).toMatchObject({ resultStatus: processingResult.status })
    expect(counterCalls[0]?.value).toBe(1)
  })

  it('registers base labels alongside resultStatus', () => {
    // Given
    const counterCalls = mockCounterCalls()
    const metric = new PrometheusMessageByStatusCounter<TestMessage>(
      { name: 'test_metric', helpDescription: 'test', labelNames: ['resultStatus'] },
      promClient,
    )

    // When
    metric.registerProcessedMessage(
      buildMetadata(
        { id: '1', messageType: 'test' },
        { queueName: 'my-queue', processingResult: { status: 'consumed' } },
      ),
    )

    // Then
    expect(counterCalls).toMatchInlineSnapshot(`
      [
        {
          "labels": {
            "messageType": "test",
            "queue": "my-queue",
            "resultStatus": "consumed",
            "version": undefined,
          },
          "value": 1,
        },
      ]
    `)
  })

  it('resolves version from message metadata', () => {
    // Given
    const counterCalls = mockCounterCalls()
    const metric = new PrometheusMessageByStatusCounter<TestMessage>(
      {
        name: 'test_metric',
        helpDescription: 'test',
        labelNames: ['resultStatus'],
        messageVersion: (metadata) => metadata.message?.metadata?.schemaVersion,
      },
      promClient,
    )
    const messages: TestMessage[] = [
      { id: '1', messageType: 'test' },
      { id: '2', messageType: 'test', metadata: { schemaVersion: '2.0.0' } },
    ]

    // When
    for (const message of messages) {
      metric.registerProcessedMessage(
        buildMetadata(message, { processingResult: { status: 'consumed' } }),
      )
    }

    // Then
    expect(counterCalls).toMatchInlineSnapshot(`
      [
        {
          "labels": {
            "messageType": "test",
            "queue": "test-queue",
            "resultStatus": "consumed",
            "version": undefined,
          },
          "value": 1,
        },
        {
          "labels": {
            "messageType": "test",
            "queue": "test-queue",
            "resultStatus": "consumed",
            "version": "2.0.0",
          },
          "value": 1,
        },
      ]
    `)
  })
})
