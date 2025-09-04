import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void

// Only topic is required for the stream to work properly
type MessageWithTopic = { topic: string }

export type KafkaMessageBatchOptions = {
  batchSize?: number
  timeoutMilliseconds?: number
}

export const KAFKA_DEFAULT_BATCH_SIZE = 1000
export const KAFKA_DEFAULT_BATCH_TIMEOUT_MS = 1000

export type MessageBatch<Message> = { topic: string; messages: Message[] }

export interface KafkaMessageBatchStream<TypedMessage extends MessageWithTopic> extends Duplex {
  // biome-ignore  lint/suspicious/noExplicitAny: compatible with Duplex definition
  on(event: string | symbol, listener: (...args: any[]) => void): this
  on(event: 'data', listener: (chunk: MessageBatch<TypedMessage>) => void): this

  push(chunk: MessageBatch<TypedMessage> | null): boolean
}

/**
 * Collects messages in batches based on provided batchSize and flushes them when messages amount or timeout is reached.
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: merging interface with class to add strong typing for 'data' event
export class KafkaMessageBatchStream<TypedMessage extends MessageWithTopic> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private readonly currentBatchPerTopic: Record<string, TypedMessage[]>
  private readonly batchTimeoutPerTopic: Record<string, NodeJS.Timeout>

  constructor(options: { batchSize: number; timeoutMilliseconds: number }) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.currentBatchPerTopic = {}
    this.batchTimeoutPerTopic = {}
  }

  override _read() {
    // No-op, as we push data when we have a full batch or timeout
  }

  override _write(chunk: TypedMessage, _encoding: BufferEncoding, callback: CallbackFunction) {
    if (!this.currentBatchPerTopic[chunk.topic]) {
      this.currentBatchPerTopic[chunk.topic] = [chunk]
    } else {
      // biome-ignore lint/style/noNonNullAssertion: non-existing entry is handled above
      this.currentBatchPerTopic[chunk.topic]!.push(chunk)
    }

    // biome-ignore lint/style/noNonNullAssertion: we ensure above that the array is defined
    if (this.currentBatchPerTopic[chunk.topic]!.length >= this.batchSize) {
      this.flushCurrentBatchMessages(chunk.topic)
      return callback(null)
    }

    if (!this.batchTimeoutPerTopic[chunk.topic]) {
      this.batchTimeoutPerTopic[chunk.topic] = setTimeout(() => {
        this.flushCurrentBatchMessages(chunk.topic)
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
    for (const topic of Object.keys(this.currentBatchPerTopic)) {
      this.flushCurrentBatchMessages(topic)
    }
  }

  private flushCurrentBatchMessages(topic: string) {
    if (this.batchTimeoutPerTopic[topic]) {
      clearTimeout(this.batchTimeoutPerTopic[topic])
      delete this.batchTimeoutPerTopic[topic]
    }

    if (!this.currentBatchPerTopic[topic]?.length) {
      return
    }

    this.push({ topic, messages: this.currentBatchPerTopic[topic] })
    this.currentBatchPerTopic[topic] = []
  }
}
