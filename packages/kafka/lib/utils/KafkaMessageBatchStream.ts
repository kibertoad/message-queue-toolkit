import { Transform } from 'node:stream'

// Topic and partition are required for the stream to work properly
type MessageWithTopicAndPartition = { topic: string; partition: number }

export type KafkaMessageBatchOptions = {
  batchSize: number
  timeoutMilliseconds: number
}

export type MessageBatch<TMessage> = { topic: string; partition: number; messages: TMessage[] }
export type OnMessageBatchCallback<TMessage> = (batch: MessageBatch<TMessage>) => Promise<void>

/**
 * Collects messages in batches based on provided batchSize and flushes them when messages amount or timeout is reached.
 *
 * This implementation uses Transform stream which properly handles backpressure by design.
 * When the downstream consumer is slow, the stream will automatically pause accepting new messages
 * until the consumer catches up, preventing memory leaks and OOM errors.
 */
export class KafkaMessageBatchStream<
  TMessage extends MessageWithTopicAndPartition,
> extends Transform {
  private readonly batchSize: number
  private readonly timeout: number

  private readonly currentBatchPerTopicPartition: Record<string, TMessage[]>
  private readonly batchTimeoutPerTopicPartition: Record<string, NodeJS.Timeout | undefined>
  constructor(options: { batchSize: number; timeoutMilliseconds: number }) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.currentBatchPerTopicPartition = {}
    this.batchTimeoutPerTopicPartition = {}
  }

  override _transform(message: TMessage, _encoding: BufferEncoding, callback: () => void) {
    const key = getTopicPartitionKey(message.topic, message.partition)

    // Accumulate the message
    if (!this.currentBatchPerTopicPartition[key]) this.currentBatchPerTopicPartition[key] = []
    this.currentBatchPerTopicPartition[key].push(message)

    // Check if the batch is complete by size
    if (this.currentBatchPerTopicPartition[key].length >= this.batchSize) {
      this.flushCurrentBatchMessages(message.topic, message.partition)
      callback()
      return
    }

    // Start timeout for this partition if not already started
    if (!this.batchTimeoutPerTopicPartition[key]) {
      this.batchTimeoutPerTopicPartition[key] = setTimeout(
        () => this.flushCurrentBatchMessages(message.topic, message.partition),
        this.timeout,
      )
    }

    callback()
  }

  // Flush all remaining batches when stream is closing
  override async _flush(callback: () => void) {
    await this.flushAllBatches()
    callback()
  }

  private flushAllBatches() {
    for (const key of Object.keys(this.currentBatchPerTopicPartition)) {
      const { topic, partition } = splitTopicPartitionKey(key)
      this.flushCurrentBatchMessages(topic, partition)
    }
  }

  private flushCurrentBatchMessages(topic: string, partition: number) {
    const key = getTopicPartitionKey(topic, partition)

    // Clear timeout
    if (this.batchTimeoutPerTopicPartition[key]) {
      clearTimeout(this.batchTimeoutPerTopicPartition[key])
      this.batchTimeoutPerTopicPartition[key] = undefined
    }

    const messages = this.currentBatchPerTopicPartition[key] ?? []
    if (!messages.length) return

    this.push({ topic, partition, messages: messages })
    this.currentBatchPerTopicPartition[key] = []
  }

  override push(
    chunk: { topic: string; partition: number; messages: TMessage[] },
    encoding?: BufferEncoding,
  ): boolean {
    return super.push(chunk, encoding)
  }
}

const getTopicPartitionKey = (topic: string, partition: number): string => `${topic}:${partition}`
const splitTopicPartitionKey = (key: string): { topic: string; partition: number } => {
  const [topic, partition] = key.split(':')
  /* v8 ignore start */
  if (!topic || !partition) throw new Error('Invalid topic-partition key format')
  /* v8 ignore stop */

  return { topic, partition: Number.parseInt(partition, 10) }
}
