import { Transform, type TransformCallback } from 'node:stream'

// Topic and partition are required for the stream to work properly
type MessageWithTopicAndPartition = { topic: string; partition: number }

export type KafkaMessageBatchOptions = {
  batchSize: number
  timeoutMilliseconds: number
}

export type MessageBatch<TMessage> = { topic: string; partition: number; messages: TMessage[] }

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

  private readonly messages: TMessage[]
  private existingTimeout: NodeJS.Timeout | undefined

  constructor(options: { batchSize: number; timeoutMilliseconds: number }) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds

    this.messages = []
  }

  override _transform(message: TMessage, _encoding: BufferEncoding, callback: TransformCallback) {
    try {
      this.messages.push(message)

      // Check if the batch is complete by size
      if (this.messages.length >= this.batchSize) {
        this.flushMessages()
        return
      } else if (!this.existingTimeout) {
        // Start timeout if not already started
        this.existingTimeout = setTimeout(() => this.flushMessages(), this.timeout)
      }
    } finally {
      callback(null)
    }
  }

  // Flush all remaining batches when stream is closing
  override _flush(callback: () => void) {
    this.flushMessages()
    this.push(null) // end of stream
    callback()
  }

  private flushMessages() {
    if (this.existingTimeout) {
      clearTimeout(this.existingTimeout)
      this.existingTimeout = undefined
    }

    const messages = this.messages.splice(0, this.messages.length)
    if (messages.length) {
      this.push(messages)
    }
  }

  override push(chunk: TMessage[] | null, encoding?: BufferEncoding): boolean {
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
