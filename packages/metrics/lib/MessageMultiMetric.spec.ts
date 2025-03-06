import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import * as promClient from 'prom-client'
import { describe, expect, it } from 'vitest'
import { MessageMultiMetric } from './MessageMultiMetric'
import { MessageLifetimeMetric } from './prometheus/metrics/MessageLifetimeMetric'
import { MessageProcessingTimeMetric } from './prometheus/metrics/MessageProcessingTimeMetric'

type TestMessage = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('MessageMultiMetric', () => {
  it('registers multiple metrics', () => {
    // Given
    const registeredProcessingTimeValues: ProcessedMessageMetadata<TestMessage>[] = []
    const registeredLifetimeValues: ProcessedMessageMetadata<TestMessage>[] = []

    const processingTimeMetric = new MessageProcessingTimeMetric<TestMessage>(
      {
        name: 'test_processing_time',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessage>) => {
          registeredProcessingTimeValues.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        },
      },
      promClient,
    )

    const lifetimeMetric = new MessageLifetimeMetric<TestMessage>(
      {
        name: 'test_processing_time',
        helpDescription: 'test description',
        buckets: [1, 2, 3],
        messageVersion: (metadata: ProcessedMessageMetadata<TestMessage>) => {
          registeredLifetimeValues.push(metadata) // Mocking it to check if value is registered properly
          return undefined
        },
      },
      promClient,
    )

    const multiMetric = new MessageMultiMetric<TestMessage>([processingTimeMetric, lifetimeMetric])

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
      multiMetric.registerProcessedMessage(processedMessageMetadata)
    }

    // Then
    expect(registeredProcessingTimeValues).toStrictEqual(processedMessageMetadataEntries)
    expect(registeredLifetimeValues).toStrictEqual(processedMessageMetadataEntries)
  })
})
