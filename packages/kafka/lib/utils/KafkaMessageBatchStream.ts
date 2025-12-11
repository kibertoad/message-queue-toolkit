import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void

// Topic and partition are required for the stream to work properly
type MessageWithTopicAndPartition = { topic: string; partition: number }

export type KafkaMessageBatchOptions = {
  batchSize: number
  timeoutMilliseconds: number
}

export type MessageBatch<TMessage> = { topic: string; partition: number; messages: TMessage[] }

export interface KafkaMessageBatchStream<TMessage extends MessageWithTopicAndPartition>
  extends Duplex {
  // biome-ignore lint/suspicious/noExplicitAny: compatible with Duplex definition
  on(event: string | symbol, listener: (...args: any[]) => void): this
  on(event: 'data', listener: (chunk: MessageBatch<TMessage>) => void): this

  push(chunk: MessageBatch<TMessage> | null): boolean
}

/**
 * Collects messages in batches based on provided batchSize and flushes them when messages amount or timeout is reached.
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: merging interface with class to add strong typing for 'data' event
export class KafkaMessageBatchStream<TMessage extends MessageWithTopicAndPartition> extends Duplex {
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

  override _read() {
    // No-op, as we push data when we have a full batch or timeout
  }

  override _write(message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction) {
    const key = this.getTopicPartitionKey(message.topic, message.partition)

    if (!this.currentBatchPerTopicPartition[key]) {
      this.currentBatchPerTopicPartition[key] = [message]
    } else {
      // biome-ignore lint/style/noNonNullAssertion: non-existing entry is handled above
      this.currentBatchPerTopicPartition[key]!.push(message)
    }

    // biome-ignore lint/style/noNonNullAssertion: we ensure above that the array is defined
    if (this.currentBatchPerTopicPartition[key]!.length >= this.batchSize) {
      this.flushCurrentBatchMessages(message.topic, message.partition)
      return callback(null)
    }

    if (!this.batchTimeoutPerTopicPartition[key]) {
      this.batchTimeoutPerTopicPartition[key] = setTimeout(() => {
        this.flushCurrentBatchMessages(message.topic, message.partition)
      }, this.timeout)
    }

    callback(null)
  }

  // Write side is closed, flush the remaining messages
  override _final(callback: CallbackFunction) {
    this.flushAllBatches()
    this.push(null) // End readable side
    callback()
  }

  private flushAllBatches() {
    for (const key of Object.keys(this.currentBatchPerTopicPartition)) {
      const { topic, partition } = this.splitTopicPartitionKey(key)
      this.flushCurrentBatchMessages(topic, partition)
    }
  }

  private flushCurrentBatchMessages(topic: string, partition: number) {
    const key = this.getTopicPartitionKey(topic, partition)

    if (this.batchTimeoutPerTopicPartition[key]) {
      clearTimeout(this.batchTimeoutPerTopicPartition[key])
      this.batchTimeoutPerTopicPartition[key] = undefined
    }

    if (!this.currentBatchPerTopicPartition[key]?.length) {
      return
    }

    this.push({ topic, partition, messages: this.currentBatchPerTopicPartition[key] })
    this.currentBatchPerTopicPartition[key] = []
  }

  private getTopicPartitionKey(topic: string, partition: number): string {
    return `${topic}:${partition}`
  }

  private splitTopicPartitionKey(key: string): { topic: string; partition: number } {
    const [topic, partition] = key.split(':')
    if (!topic || !partition) {
      throw new Error('Invalid topic-partition key format')
    }
    return { topic, partition: Number.parseInt(partition, 10) }
  }
}
