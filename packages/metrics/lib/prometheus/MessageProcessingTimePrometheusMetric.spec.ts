import * as promClient from 'prom-client'
import type { Histogram } from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { MessageProcessingTimePrometheusMetric } from './MessageProcessingTimePrometheusMetric'
import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core';

type TestMessageSchema = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('MessageProcessingTimePrometheusMetric', () => {
  it('creates and uses Histogram metric properly', () => {
    // Given
    const registeredMessages: ProcessedMessageMetadata<TestMessageSchema>[] = []
    const metric = new MessageProcessingTimePrometheusMetric<TestMessageSchema>(
      {
        name: 'test_metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessageSchema>) => {
          registeredMessages.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        }
      },
      promClient,
    )

    // When
    const messages: TestMessageSchema[] = [
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

    const processedMessageMetadataEntries: ProcessedMessageMetadata<TestMessageSchema>[] = messages.map(message => ({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: 'consumed',
      message: message,
      messageProcessingMilliseconds: 10,
    }))

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

    const metric = new MessageProcessingTimePrometheusMetric<TestMessageSchema>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
      },
      promClient,
    )

    // When
    const message: TestMessageSchema = {
      id: '1',
      messageType: 'test',
      timestamp: new Date().toISOString(),
    }

    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: 'consumed',
      message: message,
      messageProcessingMilliseconds: 111,
    })

    // Then
    expect(observedValues).toStrictEqual([
      {
        labels: {
          messageType: 'test',
          version: undefined,
        },
        value: 111,
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

    const metric = new MessageProcessingTimePrometheusMetric<TestMessageSchema>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessageSchema>) =>
          metadata.message?.metadata?.schemaVersion,
      },
      promClient,
    )

    // When
    const messages: TestMessageSchema[] = [
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
      metric.registerProcessedMessage({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: 'consumed',
        message: message,
        messageProcessingMilliseconds: 10,
      })
    }

    // Then
    expect(observedValues).toStrictEqual([
      {
        labels: {
          messageType: 'test',
          version: undefined,
        },
        value: 10,
      },
      {
        labels: {
          messageType: 'test',
          version: '1.0.0',
        },
        value: 10,
      },
    ])
  })

  it('skips observation if message processing time is not available', () => {
    // Given
    const observedValues: { labels: Record<string, any>; value: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      observe(labels: Record<string, string | number>, value: number) {
        observedValues.push({ labels, value })
      },
    } as Histogram)

    const metric = new MessageProcessingTimePrometheusMetric<TestMessageSchema>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
      },
      promClient,
    )

    // When
    const message: TestMessageSchema = {
      id: '1',
      messageType: 'test',
      timestamp: new Date().toISOString(),
    }

    metric.registerProcessedMessage({
      messageId: message.id,
      messageType: message.messageType,
      processingResult: 'consumed',
      message: message,
    })

    // Then
    expect(observedValues).toStrictEqual([])
  })
})
