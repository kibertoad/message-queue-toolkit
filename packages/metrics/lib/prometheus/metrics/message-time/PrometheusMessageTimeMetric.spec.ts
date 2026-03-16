import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Histogram, LabelValues } from 'prom-client'
import * as promClient from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric.ts'

type TestMessage = {
  id: string
  messageType: 'test'
}

// Concrete implementation with no custom labels
class TestTimeMetric extends PrometheusMessageTimeMetric<TestMessage> {
  protected calculateObservedValue(metadata: ProcessedMessageMetadata<TestMessage>): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }
}

// Concrete implementation with custom labels
class TestTimeMetricWithLabels extends PrometheusMessageTimeMetric<TestMessage, 'env'> {
  protected calculateObservedValue(metadata: ProcessedMessageMetadata<TestMessage>): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }

  protected override getLabelValuesForProcessedMessage(): LabelValues<'env'> {
    return { env: 'production' }
  }
}

const mockObservedValues = () => {
  const observedValues: { labels: Record<string, unknown>; value: number }[] = []
  vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
    observe(labels: Record<string, string | number>, value: number) {
      observedValues.push({ labels, value })
    },
  } as Histogram)
  return observedValues
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
  messageProcessingStartTimestamp: 1000,
  messageProcessingEndTimestamp: 1100,
  ...overrides,
})

describe('PrometheusMessageTimeMetric', () => {
  describe('labelNames', () => {
    it('allows omitting labelNames when no custom Labels type is defined', () => {
      expect(
        () =>
          new TestTimeMetric(
            { name: 'test_histogram', helpDescription: 'test', buckets: [10, 50, 100] },
            promClient,
          ),
      ).not.toThrow()
    })

    it('registers only base labels when no custom labels are defined', () => {
      // Given
      const observedValues = mockObservedValues()
      const metric = new TestTimeMetric(
        { name: 'test_histogram', helpDescription: 'test', buckets: [10, 50, 100] },
        promClient,
      )

      // When
      metric.registerProcessedMessage(buildMetadata({ id: '1', messageType: 'test' }))

      // Then
      expect(observedValues).toMatchInlineSnapshot(`
        [
          {
            "labels": {
              "messageType": "test",
              "queue": "test-queue",
              "result": "consumed",
              "version": undefined,
            },
            "value": 100,
          },
        ]
      `)
    })
  })

  it('registers custom labels alongside base labels', () => {
    // Given
    const observedValues = mockObservedValues()
    const metric = new TestTimeMetricWithLabels(
      {
        name: 'test_histogram_labels',
        helpDescription: 'test',
        buckets: [10, 50, 100],
        labelNames: ['env'],
      },
      promClient,
    )

    // When
    metric.registerProcessedMessage(buildMetadata({ id: '1', messageType: 'test' }))

    // Then
    expect(observedValues).toMatchInlineSnapshot(`
      [
        {
          "labels": {
            "env": "production",
            "messageType": "test",
            "queue": "test-queue",
            "result": "consumed",
            "version": undefined,
          },
          "value": 100,
        },
      ]
    `)
  })
})
