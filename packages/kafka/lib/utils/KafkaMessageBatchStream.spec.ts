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
      [messages[2]], // partition 0
      [messages[3]], // partition 1
      [messages[4], messages[5]], // partition 1
      [messages[6]], // partition 0
      [messages[7]], // partition 1
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

  describe('backpressure', () => {
    it('should pause writes when the readable buffer is full', async () => {
      // With batchSize=1, each message is immediately flushed to the readable buffer.
      // The objectMode readableHighWaterMark defaults to 16:
      // push() returns false on the 16th flush, causing the _write callback to be held
      // until _read() signals the downstream is ready again.
      const batchStream = new KafkaMessageBatchStream<any>({
        batchSize: 1,
        timeoutMilliseconds: 10000,
      })

      const processedmessages = new Set<number>()
      // Write 15 messages: push() returns true for each (buffer 1→15, below HWM of 16)
      for (let i = 0; i < 15; i++) {
        await new Promise<void>((resolve) =>
          batchStream.write({ id: i, topic: 'test', partition: 0 }, () => {
            processedmessages.add(i)
            resolve()
          }),
        )
      }

      // 16th write fills the buffer to HWM: push() returns false, callback is held
      let sixteenthWriteCompleted = false
      batchStream.write({ id: 15, topic: 'test', partition: 0 }, () => {
        processedmessages.add(15)
        sixteenthWriteCompleted = true
      })

      await setTimeout(10)
      expect(processedmessages.size).toEqual(15)
      expect(sixteenthWriteCompleted).toBe(false) // write is paused by backpressure

      // Consuming one item triggers _read(), releasing the held callback
      batchStream.read()

      await setTimeout(10)
      expect(processedmessages.size).toEqual(16)
      expect(sixteenthWriteCompleted).toBe(true) // write resumes
    })

    it('should deliver all messages without loss when the consumer is slow', async () => {
      // 20 messages with batchSize=1 generates 20 batches.
      // HWM is 16, so batches 17-20 are held until the consumer drains the buffer,
      // verifying that backpressure does not cause message loss.
      const topic = 'test-topic'
      const totalMessages = 20
      const messages = Array.from({ length: totalMessages }, (_, i) => ({
        id: i,
        topic,
        partition: 0,
      }))

      const batchStream = new KafkaMessageBatchStream<any>({
        batchSize: 1,
        timeoutMilliseconds: 10000,
      })

      const receivedIds: number[] = []
      let resolveAll!: () => void
      const allReceived = new Promise<void>((resolve) => {
        resolveAll = resolve
      })

      // Slow consumer: each batch takes longer than writes are produced
      const consume = async () => {
        for await (const batch of batchStream) {
          await setTimeout(10)
          for (const msg of batch) receivedIds.push(msg.id)
          if (receivedIds.length >= totalMessages) {
            resolveAll()
            break
          }
        }
      }
      void consume()

      for (const msg of messages) batchStream.write(msg)

      await allReceived
      expect(receivedIds).toHaveLength(totalMessages)
      expect(receivedIds).toEqual(messages.map((m) => m.id))
    })

    it('should respect a custom readableHighWaterMark', async () => {
      // With readableHighWaterMark=1 and batchSize=1, push() returns false after the
      // very first item in the readable buffer, triggering backpressure immediately.
      const batchStream = new KafkaMessageBatchStream<any>({
        batchSize: 1,
        timeoutMilliseconds: 10000,
        readableHighWaterMark: 1,
      })

      // First write: flushed immediately, buffer reaches HWM=1, push() returns false
      // → _write callback is held by backpressure
      let firstWriteCompleted = false
      batchStream.write({ id: 0, topic: 'test', partition: 0 }, () => {
        firstWriteCompleted = true
      })

      await setTimeout(10)
      expect(firstWriteCompleted).toBe(false) // held by backpressure

      // Consuming one item triggers _read(), releasing the held callback
      batchStream.read()

      await setTimeout(10)
      expect(firstWriteCompleted).toBe(true) // write resumes
    })

    it('should defer timeout flush when backpressured and flush once consumer reads', () => {
      vi.useFakeTimers()

      const topic = 'test-topic'
      const batchStream = new KafkaMessageBatchStream<any>({
        batchSize: 1000, // large: only timeout-based flushes
        timeoutMilliseconds: 100,
      })

      // Mock push() to simulate a full readable buffer on the first flush
      let simulateBackpressure = true
      const originalPush = batchStream.push.bind(batchStream)
      vi.spyOn(batchStream, 'push').mockImplementation((chunk) => {
        originalPush(chunk) // still push the data to the buffer
        return !simulateBackpressure // return false to signal backpressure
      })

      // No 'data' listener: stream stays in paused mode so _read() is only
      // triggered when batchStream.read() is called explicitly

      // Write 5 messages: they accumulate in this.messages, a timeout is scheduled
      for (let i = 0; i < 5; i++) {
        batchStream.write({ id: i, topic, partition: 0 })
      }

      // First timeout fires: flushMessages() → push() returns false → isBackPreassured = true
      vi.advanceTimersByTime(100)
      expect(batchStream.readableLength).toBe(1) // 1 batch buffered (the 5 messages)

      // Write 3 more messages: they accumulate in this.messages while backpressured
      for (let i = 5; i < 8; i++) {
        batchStream.write({ id: i, topic, partition: 0 })
      }

      // Second timeout fires: isBackPreassured is true → reschedules several times without flushing
      vi.advanceTimersByTime(300)
      expect(batchStream.readableLength).toBe(1) // unchanged: no new flush happened

      // Consumer reads one item → triggers _read() → isBackPreassured = false
      simulateBackpressure = false
      const firstBatch = batchStream.read()
      expect(firstBatch).toHaveLength(5)
      expect(batchStream.readableLength).toBe(0)

      // Third timeout fires: isBackPreassured is false → flushes the 3 accumulated messages
      vi.advanceTimersByTime(100)
      expect(batchStream.readableLength).toBe(1) // new batch pushed

      const secondBatch = batchStream.read()
      expect(secondBatch).toHaveLength(3) // messages 5–7

      vi.useRealTimers()
    })
  })
})
