import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Counter, LabelValues } from 'prom-client'
import * as promClient from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

type TestMessage = {
  id: string
  messageType: 'test'
}

// Concrete implementation with no custom labels
class TestCounter extends PrometheusMessageCounter<TestMessage> {
  protected calculateCount(metadata: ProcessedMessageMetadata<TestMessage>): number | null {
    return metadata.processingResult.status === 'consumed' ? 1 : null
  }
}

// Concrete implementation with custom labels
class TestCounterWithLabels extends PrometheusMessageCounter<TestMessage, 'result'> {
  protected override getLabelValuesForProcessedMessage(
    metadata: ProcessedMessageMetadata<TestMessage>,
  ): LabelValues<'result'> {
    return { result: metadata.processingResult.status }
  }

  protected calculateCount(): number | null {
    return 1
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

describe('PrometheusMessageCounter', () => {
  describe('labelNames', () => {
    it('allows omitting labelNames when no custom Labels type is defined', () => {
      // No TypeScript error and no runtime error when labelNames is omitted
      expect(
        () => new TestCounter({ name: 'test_counter', helpDescription: 'test' }, promClient),
      ).not.toThrow()
    })

    it('registers only base labels when no custom labels are defined', () => {
      // Given
      const counterCalls = mockCounterCalls()
      const metric = new TestCounter({ name: 'test_counter', helpDescription: 'test' }, promClient)
      const message: TestMessage = { id: '1', messageType: 'test' }

      // When
      metric.registerProcessedMessage({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: { status: 'consumed' },
        message,
        queueName: 'test-queue',
        messageTimestamp: Date.now(),
        messageProcessingStartTimestamp: Date.now(),
        messageProcessingEndTimestamp: Date.now(),
      })

      // Then
      expect(counterCalls).toMatchInlineSnapshot(`
        [
          {
            "labels": {
              "messageType": "test",
              "queue": "test-queue",
              "version": undefined,
            },
            "value": 1,
          },
        ]
      `)
    })
  })

  it('skips increment when calculateCount returns null', () => {
    // Given
    const counterCalls = mockCounterCalls()
    const metric = new TestCounter({ name: 'test_counter', helpDescription: 'test' }, promClient)
    const message: TestMessage = { id: '1', messageType: 'test' }

    // When
    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: { status: 'error', errorReason: 'invalidMessage' },
      message,
      queueName: 'test-queue',
      messageTimestamp: Date.now(),
      messageProcessingStartTimestamp: Date.now(),
      messageProcessingEndTimestamp: Date.now(),
    })

    // Then
    expect(counterCalls).toHaveLength(0)
  })

  it('registers custom labels when Labels type is defined', () => {
    // Given
    const counterCalls = mockCounterCalls()
    const metric = new TestCounterWithLabels(
      { name: 'test_counter_labels', helpDescription: 'test', labelNames: ['result'] },
      promClient,
    )
    const message: TestMessage = { id: '1', messageType: 'test' }

    // When
    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: { status: 'consumed' },
      message,
      queueName: 'test-queue',
      messageTimestamp: Date.now(),
      messageProcessingStartTimestamp: Date.now(),
      messageProcessingEndTimestamp: Date.now(),
    })

    // Then
    expect(counterCalls).toMatchInlineSnapshot(`
      [
        {
          "labels": {
            "messageType": "test",
            "queue": "test-queue",
            "result": "consumed",
            "version": undefined,
          },
          "value": 1,
        },
      ]
    `)
  })
})
