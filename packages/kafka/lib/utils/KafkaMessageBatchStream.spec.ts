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

    const batchStream = new KafkaMessageBatchStream<any>({
      batchSize: 3,
      timeoutMilliseconds: 10000,
    }) // Setting big timeout to check batch size only

    batchStream.on('data', (batch) => {
      receivedBatches.push(batch)
      // We expect 3 batches and the last message waiting in the stream
      if (receivedBatches.length >= 3) {
        resolvePromise()
      }
    })

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

    const batchStream = new KafkaMessageBatchStream<any>({
      batchSize: 1000,
      timeoutMilliseconds: 100,
    }) // Setting big batch size to check timeout only

    batchStream.on('data', (batch) => {
      receivedBatches.push(batch)
    })

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

    const batchStream = new KafkaMessageBatchStream<{ topic: string; partition: number }>({
      batchSize: 2,
      timeoutMilliseconds: 10000,
    }) // Setting big timeout to check batch size only

    batchStream.on('data', (batch) => {
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
    })

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

    const batchStream = new KafkaMessageBatchStream<{ topic: string; partition: number }>({
      batchSize: 2,
      timeoutMilliseconds: 10000,
    }) // Setting big timeout to check batch size only

    batchStream.on('data', (batch) => {
      receivedBatches.push(batch)

      // We expect 4 batches (2 per partition)
      receivedBatchesCounter++
      if (receivedBatchesCounter >= 4) {
        resolvePromise()
      }
    })

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

  // Skipping for beta
  it.skip('should emit batches based on timeout with delayed writes', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 6 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
      partition: 0,
    }))

    const batchMessageCounts: number[] = [] // Track number of messages per batch

    const batchStream = new KafkaMessageBatchStream<any>({
      batchSize: 1000, // Large batch size to never trigger size-based flushing
      timeoutMilliseconds: 30, // Short timeout to trigger flush
    })

    batchStream.on('data', (batch) => {
      batchMessageCounts.push(batch.messages.length)
    })

    // When: Write messages with 20ms delay between them
    for (const message of messages) {
      batchStream.write(message)
      await setTimeout(20)
    }

    // Then: Wait for batches to be emitted
    await waitAndRetry(() => batchMessageCounts.length >= 6, 500, 20)

    // Each message triggers its own timeout flush since they arrive 20ms apart
    expect(batchMessageCounts.length).toBeGreaterThanOrEqual(1)
    expect(batchMessageCounts.reduce((a, b) => a + b, 0)).toBe(6)
  })
})
