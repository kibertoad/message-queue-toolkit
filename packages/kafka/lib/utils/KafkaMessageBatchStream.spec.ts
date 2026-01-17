import { setTimeout } from 'node:timers/promises'
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
    expect(receivedBatches).toEqual([messages])
  })
})
