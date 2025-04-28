import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import * as promClient from 'prom-client'
import type { Counter } from 'prom-client'
import { describe, expect, it, vi } from 'vitest'
import { PrometheusMessageErrorCounter } from './PrometheusMessageErrorCounter.ts'

type TestMessage = {
  id: string
  messageType: 'test'
  timestamp?: string
  metadata?: {
    schemaVersion: string
  }
}

describe('PrometheusMessageErrorCounter', () => {
  it('creates and uses Counter metric properly', () => {
    // Given
    const registeredMessages: ProcessedMessageMetadata<TestMessage>[] = []
    const metric = new PrometheusMessageErrorCounter<TestMessage>(
      {
        name: 'test_metric',
        helpDescription: 'test description',
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
        metadata: { schemaVersion: '1.0.0' },
      },
    ]

    const processedMessageMetadataEntries: ProcessedMessageMetadata<TestMessage>[] = messages.map(
      (message) => ({
        messageId: message.id,
        messageType: message.messageType,
        processingResult: { status: 'error', errorReason: 'invalidMessage' },
        message: message,
        queueName: 'test-queue',
        messageTimestamp: Date.now(),
        messageProcessingStartTimestamp: Date.now(),
        messageProcessingEndTimestamp: Date.now(),
      }),
    )

    for (const processedMessageMetadata of processedMessageMetadataEntries) {
      metric.registerProcessedMessage(processedMessageMetadata)
    }

    // Then
    expect(registeredMessages).toStrictEqual(processedMessageMetadataEntries)
  })

  const mockMetric = () => {
    const counterCalls: { labels: Record<string, unknown>; value?: number }[] = []
    vi.spyOn(promClient.register, 'getSingleMetric').mockReturnValue({
      inc(labels: Record<string, string | number>, value?: number) {
        counterCalls.push({ labels, value })
      },
    } as Counter)

    return counterCalls
  }

  it.each(['consumed', 'published', 'retryLater'])('ignores non error cases', (status: string) => {
    // Given
    const counterCalls = mockMetric()

    const metric = new PrometheusMessageErrorCounter<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
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
      processingResult: { status: status as any },
      message: message,
      queueName: 'test-queue',
      messageTimestamp: Date.now(),
      messageProcessingStartTimestamp: Date.now(),
      messageProcessingEndTimestamp: Date.now(),
    })

    // Then
    expect(counterCalls).toHaveLength(0)
  })

  it('registers values properly', () => {
    // Given
    const counterCalls = mockMetric()

    const metric = new PrometheusMessageErrorCounter<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
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
      processingResult: { status: 'error', errorReason: 'handlerError' },
      message: message,
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
            "errorReason": "handlerError",
            "messageType": "test",
            "queue": "test-queue",
            "version": undefined,
          },
          "value": 1,
        },
      ]
    `)
  })

  it('resolves version properly', () => {
    // Given
    const counterCalls = mockMetric()

    const metric = new PrometheusMessageErrorCounter<TestMessage>(
      {
        name: 'Test metric',
        helpDescription: 'test description',
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
        processingResult: { status: 'error', errorReason: 'retryLaterExceeded' },
        message: message,
        queueName,
        messageTimestamp: Date.now(),
        messageProcessingStartTimestamp: timestamp,
        messageProcessingEndTimestamp: timestamp + 53,
      })
    }

    // Then
    expect(counterCalls).toMatchInlineSnapshot(`
      [
        {
          "labels": {
            "errorReason": "retryLaterExceeded",
            "messageType": "test",
            "queue": "test-queue",
            "version": undefined,
          },
          "value": 1,
        },
        {
          "labels": {
            "errorReason": "retryLaterExceeded",
            "messageType": "test",
            "queue": "test-queue",
            "version": "1.0.0",
          },
          "value": 1,
        },
      ]
    `)
  })
})
