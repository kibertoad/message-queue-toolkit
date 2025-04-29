import * as promClient from 'prom-client'
import type { Histogram } from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageLifetimeMetric } from './PrometheusMessageLifetimeMetric.ts'

type TestMessage = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('PrometheusMessageLifetimeMetric', () => {
  it('registers values properly', () => {
    // Given
    const observedValues: { labels: Record<string, any>; value: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      observe(labels: Record<string, string | number>, value: number) {
        observedValues.push({ labels, value })
      },
    } as Histogram)

    const metric = new PrometheusMessageLifetimeMetric<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
      },
      promClient,
    )

    // When
    const message: TestMessage = {
      id: '1',
      messageType: 'test',
      timestamp: new Date().toISOString(),
    }

    const timestamp = Date.now()
    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: { status: 'consumed' },
      message: message,
      queueName: 'test-queue',
      messageTimestamp: timestamp,
      messageProcessingStartTimestamp: timestamp + 50,
      messageProcessingEndTimestamp: timestamp + 200,
    })

    // Then
    expect(observedValues).toStrictEqual([
      {
        labels: {
          messageType: 'test',
          version: undefined,
          result: 'consumed',
          queue: 'test-queue',
        },
        value: 200,
      },
    ])
  })

  it('skips observation if message timestamp is not available', () => {
    // Given
    const observedValues: { labels: Record<string, any>; value: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      observe(labels: Record<string, string | number>, value: number) {
        observedValues.push({ labels, value })
      },
    } as Histogram)

    const metric = new PrometheusMessageLifetimeMetric<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
      },
      promClient,
    )

    // When
    const message: TestMessage = {
      id: '1',
      messageType: 'test',
      timestamp: new Date().toISOString(),
    }

    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: { status: 'consumed' },
      queueName: 'test-queue',
      message: message,
      messageTimestamp: undefined,
      messageProcessingStartTimestamp: Date.now(),
      messageProcessingEndTimestamp: Date.now(),
    })

    // Then
    expect(observedValues).toStrictEqual([])
  })
})
