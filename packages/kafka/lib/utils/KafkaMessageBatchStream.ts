import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void
type MessageWithTopicAndPartition = { topic: string; partition: number }

/**
 * Options for configuring the KafkaMessageBatchStream behavior.
 */
export type KafkaMessageBatchOptions = {
  /** Maximum number of messages to accumulate across all partitions before flushing */
  batchSize: number
  /** Time in milliseconds to wait before flushing incomplete batches */
  timeoutMilliseconds: number
}

/**
 * Interface extending Duplex to provide strong typing for the 'data' event.
 * The stream emits arrays of messages grouped by topic-partition.
 */
export interface KafkaMessageBatchStream<TMessage extends MessageWithTopicAndPartition>
  extends Duplex {
  // biome-ignore lint/suspicious/noExplicitAny: compatible with Duplex definition
  on(event: string | symbol, listener: (...args: any[]) => void): this
  /** Listen for batches of messages from the same topic-partition */
  on(event: 'data', listener: (chunk: TMessage[]) => void): this
  push(chunk: TMessage[] | null): boolean
}

/**
 * A Duplex stream that batches Kafka messages based on size and timeout constraints.
 *
 * Key features:
 * - Accumulates messages across all partitions up to `batchSize` for true memory control
 * - Groups messages by topic-partition when flushing
 * - Implements backpressure: pauses input when downstream consumers are overwhelmed
 * - Auto-flushes on timeout to prevent messages from waiting indefinitely
 *
 * @example
 * ```typescript
 * const batchStream = new KafkaMessageBatchStream({ batchSize: 100, timeoutMilliseconds: 1000 })
 * batchStream.on('data', (batch) => {
 *   console.log(`Received ${batch.length} messages from ${batch[0].topic}:${batch[0].partition}`)
 * })
 * ```
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: merging interface with class to add strong typing for 'data' event
export class KafkaMessageBatchStream<TMessage extends MessageWithTopicAndPartition> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private readonly messages: TMessage[]
  private existingTimeout: NodeJS.Timeout | undefined
  private pendingCallback: CallbackFunction | undefined
  private isBackPressured: boolean

  constructor(options: KafkaMessageBatchOptions) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.messages = []
    this.isBackPressured = false
  }

  /**
   * Called when the downstream consumer is ready to receive more data.
   * This is the backpressure release mechanism: we resume the writable side
   * by calling the pending callback that was held during backpressure.
   */
  override _read() {
    this.isBackPressured = false
    if (!this.pendingCallback) return

    const cb = this.pendingCallback
    this.pendingCallback = undefined
    cb() // Resume the writable side
  }

  /**
   * Writes a message to the stream.
   * Messages accumulate until batchSize is reached or timeout expires.
   * Implements backpressure by holding the callback when downstream cannot consume.
   */
  override _write(message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction) {
    let canContinue = true

    try {
      this.messages.push(message)

      if (this.messages.length >= this.batchSize) {
        // Batch is full, flush immediately
        canContinue = this.flushMessages()
      } else {
        // Start/continue the timeout for partial batches
        // Using ??= ensures we only set one timeout at a time
        this.existingTimeout ??= setTimeout(() => this.flushMessages(), this.timeout)
      }
    } finally {
      // Backpressure handling: hold the callback if push() returned false
      if (!canContinue) this.pendingCallback = callback
      else callback()
    }
  }

  override _final(callback: CallbackFunction) {
    this.flushMessages()
    this.push(null) // Signal end-of-stream to the readable side
    callback()
  }

  private flushMessages(): boolean {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined

    if (this.isBackPressured) {
      this.existingTimeout = setTimeout(() => this.flushMessages(), this.timeout)
      return false
    }

    // Extract all accumulated messages and clear the array
    const messageBatch = this.messages.splice(0, this.messages.length)

    // Group by topic-partition to maintain commit guarantees
    const messagesByTopicPartition: Record<string, TMessage[]> = {}
    for (const message of messageBatch) {
      const key = getTopicPartitionKey(message.topic, message.partition)
      if (!messagesByTopicPartition[key]) messagesByTopicPartition[key] = []
      messagesByTopicPartition[key].push(message)
    }

    // Push each topic-partition batch and track backpressure.
    // All batches must be pushed regardless: messages were already splice'd from the buffer,
    // so breaking early would lose them. Once push() returns false, subsequent calls in the
    // same tick also return false, so the last value correctly reflects backpressure.
    let canContinue = true
    for (const messagesForKey of Object.values(messagesByTopicPartition)) {
      canContinue = this.push(messagesForKey)
    }

    if (!canContinue) this.isBackPressured = true

    return canContinue
  }
}

const getTopicPartitionKey = (topic: string, partition: number): string => `${topic}:${partition}`
