import { setTimeout } from 'node:timers/promises'
import { KafkaMessageBatchStream } from './KafkaMessageBatchStream.ts'

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
    const receivedBatches: any[][] = []

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
      [messages[0], messages[1], messages[2]],
      [messages[3], messages[4], messages[5]],
      [messages[6], messages[7], messages[8]],
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
    const receivedBatches: any[][] = []

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
    expect(receivedBatches).toEqual([messages])
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
      const key = `${batch[0]!.topic}:${batch[0]!.partition}`
      if (!receivedBatchesByTopicPartition[key]) {
        receivedBatchesByTopicPartition[key] = []
      }
      receivedBatchesByTopicPartition[key]!.push(batch)

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

      // We expect 6 batches due to cross-partition accumulation with batchSize=2
      receivedBatchesCounter++
      if (receivedBatchesCounter >= 6) {
        resolvePromise()
      }
    })

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    // With batchSize=2, messages accumulate across all partitions:
    // - write(msg[0], msg[1]) → flush → [msg[0], msg[1]] (partition 0)
    // - write(msg[2], msg[3]) → flush → [msg[2]] (partition 0) and [msg[3]] (partition 1)
    // - write(msg[4], msg[5]) → flush → [msg[4], msg[5]] (partition 1)
    // - write(msg[6], msg[7]) → flush → [msg[6]] (partition 0) and [msg[7]] (partition 1)
    expect(receivedBatches).toEqual([
      [messages[0], messages[1]], // partition 0
      [messages[2]],              // partition 0
      [messages[3]],              // partition 1
      [messages[4], messages[5]], // partition 1
      [messages[6]],              // partition 0
      [messages[7]],              // partition 1
    ])

    expect(messages[0]!.partition).toBe(0)
    expect(messages[1]!.partition).toBe(0)
    expect(messages[2]!.partition).toBe(0)
    expect(messages[3]!.partition).toBe(1)
    expect(messages[4]!.partition).toBe(1)
    expect(messages[5]!.partition).toBe(1)
    expect(messages[6]!.partition).toBe(0)
    expect(messages[7]!.partition).toBe(1)
  })
})
