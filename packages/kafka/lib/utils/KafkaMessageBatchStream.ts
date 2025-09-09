import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void

// Only topic is required for the stream to work properly
type MessageWithTopic = { topic: string }

export type KafkaMessageBatchOptions = {
  batchSize: number
  timeoutMilliseconds: number
}

export type MessageBatch<TMessage> = { topic: string; messages: TMessage[] }

export interface KafkaMessageBatchStream<TMessage extends MessageWithTopic> extends Duplex {
  // biome-ignore  lint/suspicious/noExplicitAny: compatible with Duplex definition
  on(event: string | symbol, listener: (...args: any[]) => void): this
  on(event: 'data', listener: (chunk: MessageBatch<TMessage>) => void): this

  push(chunk: MessageBatch<TMessage> | null): boolean
}

/**
 * Collects messages in batches based on provided batchSize and flushes them when messages amount or timeout is reached.
 */
// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: merging interface with class to add strong typing for 'data' event
export class KafkaMessageBatchStream<TMessage extends MessageWithTopic> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private readonly currentBatchPerTopic: Record<string, TMessage[]>
  private readonly batchTimeoutPerTopic: Record<string, NodeJS.Timeout | undefined>

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

  override _write(message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction) {
    if (!this.currentBatchPerTopic[message.topic]) {
      this.currentBatchPerTopic[message.topic] = [message]
    } else {
      // biome-ignore lint/style/noNonNullAssertion: non-existing entry is handled above
      this.currentBatchPerTopic[message.topic]!.push(message)
    }

    // biome-ignore lint/style/noNonNullAssertion: we ensure above that the array is defined
    if (this.currentBatchPerTopic[message.topic]!.length >= this.batchSize) {
      this.flushCurrentBatchMessages(message.topic)
      return callback(null)
    }

    if (!this.batchTimeoutPerTopic[message.topic]) {
      this.batchTimeoutPerTopic[message.topic] = setTimeout(() => {
        this.flushCurrentBatchMessages(message.topic)
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
      this.batchTimeoutPerTopic[topic] = undefined
    }

    if (!this.currentBatchPerTopic[topic]?.length) {
      return
    }

    this.push({ topic, messages: this.currentBatchPerTopic[topic] })
    this.currentBatchPerTopic[topic] = []
  }
}
