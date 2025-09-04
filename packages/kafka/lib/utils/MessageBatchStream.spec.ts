import { KafkaMessageBatchStream, type MessageBatch } from './KafkaMessageBatchStream.ts'

describe('MessageBatchStream', () => {
  it('should batch messages based on batch size', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
    }))

    // When
    const batchStream = new KafkaMessageBatchStream<any>({
      batchSize: 3,
      timeoutMilliseconds: 10000,
    }) // Setting big timeout to check batch size only

    const receivedBatches: MessageBatch<any>[] = []

    const dataFetchingPromise = new Promise((resolve) => {
      batchStream.on('data', (batch) => {
        receivedBatches.push(batch)
        // We expect 3 batches and last message waiting in the stream
        if (receivedBatches.length >= 3) {
          resolve(null)
        }
      })
    })

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    expect(receivedBatches).toEqual([
      { topic, messages: [messages[0], messages[1], messages[2]] },
      { topic, messages: [messages[3], messages[4], messages[5]] },
      { topic, messages: [messages[6], messages[7], messages[8]] },
    ])
  })

  it('should batch messages based on timeout', async () => {
    // Given
    const topic = 'test-topic'
    const messages = Array.from({ length: 10 }, (_, i) => ({
      id: i + 1,
      content: `Message ${i + 1}`,
      topic,
    }))

    // When
    const batchStream = new KafkaMessageBatchStream<any>({
      batchSize: 1000,
      timeoutMilliseconds: 500,
    }) // Setting big batch size to check timeout only

    const receivedBatches: MessageBatch<any>[] = []
    batchStream.on('data', (batch) => {
      receivedBatches.push(batch)
    })

    for (const message of messages) {
      batchStream.write(message)
    }

    // Sleep 1 seconds to let the timeout trigger
    await new Promise((resolve) => {
      setTimeout(resolve, 1000)
    })

    // Then
    expect(receivedBatches).toEqual([{ topic, messages }])
  })

  it('should support multiple topics', async () => {
    // Given
    const firstTopic = 'test-topic-1'
    const secondTopic = 'test-topic-2'
    const thirdTopic = 'test-topic-3'
    const messages = [
      ...Array.from({ length: 4 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: firstTopic,
      })),
      ...Array.from({ length: 4 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: secondTopic,
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        id: i + 1,
        content: `Message ${i + 1}`,
        topic: thirdTopic,
      })),
    ]

    // When
    const batchStream = new KafkaMessageBatchStream<{ topic: string }>({
      batchSize: 2,
      timeoutMilliseconds: 10000,
    }) // Setting big timeout to check batch size only

    const receivedBatchesByTopic: Record<string, any[][]> = {}

    let receivedMessagesCounter = 0
    const dataFetchingPromise = new Promise((resolve) => {
      batchStream.on('data', (batch) => {
        if (!receivedBatchesByTopic[batch.topic]) {
          receivedBatchesByTopic[batch.topic] = []
        }
        receivedBatchesByTopic[batch.topic]!.push(batch.messages)

        // We expect 5 batches and last message waiting in the stream
        receivedMessagesCounter++
        if (receivedMessagesCounter >= 5) {
          resolve(null)
        }
      })
    })

    for (const message of messages) {
      batchStream.write(message)
    }

    await dataFetchingPromise

    // Then
    expect(receivedBatchesByTopic[firstTopic]).toEqual([
      [messages[0], messages[1]],
      [messages[2], messages[3]],
    ])
    expect(receivedBatchesByTopic[secondTopic]).toEqual([
      [messages[4], messages[5]],
      [messages[6], messages[7]],
    ])
    expect(receivedBatchesByTopic[thirdTopic]).toEqual([[messages[8], messages[9]]])
  })
})
