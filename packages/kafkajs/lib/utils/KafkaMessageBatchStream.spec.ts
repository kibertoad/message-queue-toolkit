import { setTimeout } from 'node:timers/promises'
import { waitAndRetry } from '@lokalise/universal-ts-utils/node'
import { KafkaMessageBatchStream, type MessageBatch } from './KafkaMessageBatchStream.ts'

describe('KafkaMessageBatchStream', () => {
  it('should batch messages based on batch size', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
      partition: 0,
    }))

    // When
    const receivedBatches: MessageBatch<any>[] = []

    let resolvePromise: () => void
    const dataFetchingPromise = new Promise<void>((resolve) => {
      resolvePromise = resolve
    })

    const batchStream = new KafkaMessageBatchStream<any>(
      (batch) => {
        receivedBatches.push(batch)
        // We expect 3 batches and the last message waiting in the stream
        if (receivedBatches.length >= 3) {
          resolvePromise()
        }
        return Promise.resolve()
      },
      {
        batchSize: 3,
        timeoutMilliseconds: 10000,
      },
    ) // Setting big timeout to check batch size only

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    expect(receivedBatches).toEqual([
      { topic, partition: 0, messages: [messages[0], messages[1], messages[2]] },
      { topic, partition: 0, messages: [messages[3], messages[4], messages[5]] },
      { topic, partition: 0, messages: [messages[6], messages[7], messages[8]] },
    ])
  })

  it('should batch messages based on timeout', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
      partition: 0,
    }))

    // When
    const receivedBatches: MessageBatch<any>[] = []

    const batchStream = new KafkaMessageBatchStream<any>(
      (batch) => {
        receivedBatches.push(batch)
        return Promise.resolve()
      },
      {
        batchSize: 1000,
        timeoutMilliseconds: 100,
      },
    ) // Setting big batch size to check timeout only

    for (const message of messages) {
      batchStream.write(message)
    }

    // Sleep to let the timeout trigger
    await setTimeout(150)

    // Then
    expect(receivedBatches).toEqual([{ topic, partition: 0, messages }])
  })

  it('should support multiple topics and partitions', async () => {
    // Given
    const firstTopic = 'test-topic-1'
    const secondTopic = 'test-topic-2'
    const thirdTopic = 'test-topic-3'
    const messages = [
      ...Array.from({ length: 4 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: firstTopic,
        partition: 0,
      })),
      ...Array.from({ length: 4 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: secondTopic,
        partition: 1,
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: thirdTopic,
        partition: 0,
      })),
    ]

    // When
    const receivedBatchesByTopicPartition: Record<string, any[][]> = {}
    let receivedMessagesCounter = 0

    let resolvePromise: () => void
    const dataFetchingPromise = new Promise<void>((resolve) => {
      resolvePromise = resolve
    })

    const batchStream = new KafkaMessageBatchStream<{ topic: string; partition: number }>(
      (batch) => {
        const key = `${batch.topic}:${batch.partition}`
        if (!receivedBatchesByTopicPartition[key]) {
          receivedBatchesByTopicPartition[key] = []
        }
        receivedBatchesByTopicPartition[key]!.push(batch.messages)

        // We expect 5 batches and last message waiting in the stream
        receivedMessagesCounter++
        if (receivedMessagesCounter >= 5) {
          resolvePromise()
        }

        return Promise.resolve()
      },
      {
        batchSize: 2,
        timeoutMilliseconds: 10000,
      },
    ) // Setting big timeout to check batch size only

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    expect(receivedBatchesByTopicPartition[`${firstTopic}:0`]).toEqual([
      [messages[0], messages[1]],
      [messages[2], messages[3]],
    ])
    expect(receivedBatchesByTopicPartition[`${secondTopic}:1`]).toEqual([
      [messages[4], messages[5]],
      [messages[6], messages[7]],
    ])
    expect(receivedBatchesByTopicPartition[`${thirdTopic}:0`]).toEqual([[messages[8], messages[9]]])
  })

  it('should batch messages separately for different partitions of the same topic', async () => {
    // Given
    const topic = 'test-topic'
    const messages = [
      ...Array.from({ length: 3 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic,
        partition: 0,
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        id: i + 4,
        content: `Message ${i + 4}`,
        topic,
        partition: 1,
      })),
      {
        id: 7,
        content: `Message 7`,
        topic,
        partition: 0,
      },
      {
        id: 8,
        content: `Message 8`,
        topic,
        partition: 1,
      },
    ]

    // When
    const receivedBatches: any[] = []
    let receivedBatchesCounter = 0

    let resolvePromise: () => void
    const dataFetchingPromise = new Promise<void>((resolve) => {
      resolvePromise = resolve
    })

    const batchStream = new KafkaMessageBatchStream<{ topic: string; partition: number }>(
      (batch) => {
        receivedBatches.push(batch)

        // We expect 4 batches (2 per partition)
        receivedBatchesCounter++
        if (receivedBatchesCounter >= 4) {
          resolvePromise()
        }

        return Promise.resolve()
      },
      {
        batchSize: 2,
        timeoutMilliseconds: 10000,
      },
    ) // Setting big timeout to check batch size only

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    expect(receivedBatches).toEqual([
      { topic, partition: 0, messages: [messages[0], messages[1]] },
      { topic, partition: 1, messages: [messages[3], messages[4]] },
      { topic, partition: 0, messages: [messages[2], messages[6]] },
      { topic, partition: 1, messages: [messages[5], messages[7]] },
    ])
  })

  it('should handle backpressure correctly when timeout flush is slow', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 6 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
      partition: 0,
    }))

    const batchStartTimes: number[] = [] // Track start times of batch processing
    const batchEndTimes: number[] = [] // Track end times of batch processing
    const batchMessageCounts: number[] = [] // Track number of messages per batch
    let maxConcurrentBatches = 0 // Track max concurrent batches

    let batchesProcessing = 0
    const batchStream = new KafkaMessageBatchStream<any>(
      async (batch) => {
        batchStartTimes.push(Date.now())
        batchMessageCounts.push(batch.messages.length)

        batchesProcessing++
        maxConcurrentBatches = Math.max(maxConcurrentBatches, batchesProcessing)

        // Simulate batch processing (50ms per batch)
        await setTimeout(50)

        batchEndTimes.push(Date.now())
        batchesProcessing--
      },
      {
        batchSize: 1000, // Large batch size to never trigger size-based flushing
        timeoutMilliseconds: 10, // Short timeout to trigger flush after each message
      },
    )

    // When: Write messages with 20ms delay between them
    // Since processing (50ms) is slower than message arrival + timeout, backpressure causes accumulation
    for (const message of messages) {
      batchStream.write(message)
      await setTimeout(20)
    }

    // Then
    // Wait until all 3 batches have been processed
    await waitAndRetry(() => batchMessageCounts.length >= 3, 500, 20)

    // Backpressure causes messages to accumulate while previous batch processes:
    // - Batch 1: Message 1 (flushed at 10ms timeout)
    // - Batch 2: Messages 2-4 (accumulated during Batch 1 processing, including Message 4 arriving at ~60ms)
    // - Batch 3: Messages 5-6 (accumulated during Batch 2 processing)
    expect(batchMessageCounts).toEqual([1, 3, 2])

    // Verify that batches never processed in parallel (backpressure working)
    expect(maxConcurrentBatches).toBe(1) // Should never process more than 1 batch at a time

    // Verify that batches were processed sequentially (each starts after previous ends)
    for (let i = 1; i < batchStartTimes.length; i++) {
      const previousEndTime = batchEndTimes[i - 1]
      const currentStartTime = batchStartTimes[i]
      // The current batch must start after the previous batch finished
      expect(currentStartTime).toBeGreaterThanOrEqual(previousEndTime ?? 0)
    }
  })
})
