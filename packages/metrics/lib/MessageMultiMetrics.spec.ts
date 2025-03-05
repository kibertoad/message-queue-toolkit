import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import * as promClient from 'prom-client'
import { describe, expect, it } from 'vitest'
import { MessageMultiMetrics } from './MessageMultiMetrics'
import { MessageLifetimeMetric } from './prometheus/metrics/MessageLifetimeMetric'
import { MessageProcessingTimeMetric } from './prometheus/metrics/MessageProcessingTimeMetric'

type TestMessageSchema = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('MessageMultiMetrics', () => {
  it('registers multiple metrics', () => {
    // Given
    const registeredProcessingTimeValues: ProcessedMessageMetadata<TestMessageSchema>[] = []
    const registeredLifetimeValues: ProcessedMessageMetadata<TestMessageSchema>[] = []

    const processingTimeMetric = new MessageProcessingTimeMetric<TestMessageSchema>(
      {
        name: 'test_processing_time',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessageSchema>) => {
          registeredProcessingTimeValues.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        },
      },
      promClient,
    )

    const lifetimeMetric = new MessageLifetimeMetric<TestMessageSchema>(
      {
        name: 'test_processing_time',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessageSchema>) => {
          registeredLifetimeValues.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        },
      },
      promClient,
    )

    const multiMetric = new MessageMultiMetrics<TestMessageSchema>([
      processingTimeMetric,
      lifetimeMetric,
    ])

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

    const timestamp = Date.now()
    const processedMessageMetadataEntries: ProcessedMessageMetadata<TestMessageSchema>[] =
      messages.map((message) => ({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: { status: 'consumed' },
        message: message,
        queueName: 'test-queue',
        messageTimestamp: timestamp,
        messageProcessingStartTimestamp: timestamp,
        messageProcessingEndTimestamp: timestamp + 102,
      }))

    for (const processedMessageMetadata of processedMessageMetadataEntries) {
      multiMetric.registerProcessedMessage(processedMessageMetadata)
    }

    // Then
    expect(registeredProcessingTimeValues).toStrictEqual(processedMessageMetadataEntries)
    expect(registeredLifetimeValues).toStrictEqual(processedMessageMetadataEntries)
  })
})
