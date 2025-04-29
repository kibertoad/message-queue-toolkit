import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import * as promClient from 'prom-client'
import type { Histogram } from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageProcessingTimeMetric } from './PrometheusMessageProcessingTimeMetric.ts'

type TestMessage = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('MessageProcessingTimeMetric', () => {
  it('creates and uses Histogram metric properly', () => {
    // Given
    const registeredMessages: ProcessedMessageMetadata<TestMessage>[] = []
    const metric = new PrometheusMessageProcessingTimeMetric<TestMessage>(
      {
        name: 'test_metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessage>) => {
          registeredMessages.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        },
      },
      promClient,
    )

    // When
    const messages: TestMessage[] = [
      {
        id: '1',
        messageType: 'test',
        timestamp: new Date().toISOString(),
      },
      {
        id: '2',
        messageType: 'test',
        timestamp: new Date().toISOString(),
        metadata: {
          schemaVersion: '1.0.0',
        },
      },
    ]

    const timestamp = Date.now()
    const processedMessageMetadataEntries: ProcessedMessageMetadata<TestMessage>[] = messages.map(
      (message) => ({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: { status: 'consumed' },
        message: message,
        queueName: 'test-queue',
        messageTimestamp: timestamp,
        messageProcessingStartTimestamp: timestamp,
        messageProcessingEndTimestamp: timestamp + 102,
      }),
    )

    for (const processedMessageMetadata of processedMessageMetadataEntries) {
      metric.registerProcessedMessage(processedMessageMetadata)
    }

    // Then
    expect(registeredMessages).toStrictEqual(processedMessageMetadataEntries)
  })

  it('registers values properly', () => {
    // Given
    const observedValues: { labels: Record<string, any>; value: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      observe(labels: Record<string, string | number>, value: number) {
        observedValues.push({ labels, value })
      },
    } as Histogram)

    const metric = new PrometheusMessageProcessingTimeMetric<TestMessage>(
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
      messageProcessingStartTimestamp: timestamp,
      messageProcessingEndTimestamp: timestamp + 102,
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
        value: 102,
      },
    ])
  })

  it('resolves version properly', () => {
    // Given
    const observedValues: { labels: Record<string, any>; value: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      observe(labels: Record<string, string | number>, value: number) {
        observedValues.push({ labels, value })
      },
    } as Histogram)

    const metric = new PrometheusMessageProcessingTimeMetric<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessage>) =>
          metadata.message?.metadata?.schemaVersion,
      },
      promClient,
    )

    // When
    const queueName = 'test-queue'
    const messages: TestMessage[] = [
      {
        id: '1',
        messageType: 'test',
        timestamp: new Date().toISOString(),
      },
      {
        id: '2',
        messageType: 'test',
        timestamp: new Date().toISOString(),
        metadata: {
          schemaVersion: '1.0.0',
        },
      },
    ]

    for (const message of messages) {
      const timestamp = Date.now()
      metric.registerProcessedMessage({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: { status: 'consumed' },
        message: message,
        queueName,
        messageTimestamp: Date.now(),
        messageProcessingStartTimestamp: timestamp,
        messageProcessingEndTimestamp: timestamp + 53,
      })
    }

    // Then
    expect(observedValues).toStrictEqual([
      {
        labels: {
          messageType: 'test',
          version: undefined,
          result: 'consumed',
          queue: queueName,
        },
        value: 53,
      },
      {
        labels: {
          messageType: 'test',
          version: '1.0.0',
          result: 'consumed',
          queue: queueName,
        },
        value: 53,
      },
    ])
  })
})
