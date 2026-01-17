import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void

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
export class KafkaMessageBatchStream<TMessage> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private readonly messages: TMessage[]
  private existingTimeout: NodeJS.Timeout | undefined
  private pendingCallback: CallbackFunction | undefined
  private isFlushing: boolean = false

  constructor(options: { batchSize: number; timeoutMilliseconds: number }) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds

    this.messages = []
  }

  override _read() {
    // When _read is called, it means the downstream consumer is ready for more data
    // This is when we should resume the writable side by calling the pending callback if it exists
    if (!this.pendingCallback) return

    const cb = this.pendingCallback
    this.pendingCallback = undefined
    cb()
  }

  override _write(message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction) {
    let canContinue = true

    try {
      this.messages.push(message)

      if (this.messages.length >= this.batchSize) {
        canContinue = this.flushMessages()
      } else {
        // If backpressure happens, we don't have a callback to hold
        // The next _write will handle backpressure
        this.existingTimeout ??= setTimeout(() => this.flushMessages(), this.timeout)
      }
    } finally {
      if (!canContinue) this.pendingCallback = callback
      else callback()
    }
  }

  override _final(callback: CallbackFunction) {
    this.flushMessages()
    this.push(null) // End readable side
    callback()
  }

  private flushMessages(): boolean {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined

    if (this.isFlushing) return true
    this.isFlushing = true

    const messages = this.messages.splice(0, this.messages.length)
    let canContinue = true
    if (messages.length) canContinue = this.push(messages)
    this.isFlushing = false

    return canContinue
  }

  override push(chunk: TMessage[] | null, encoding?: BufferEncoding): boolean {
    return super.push(chunk, encoding)
  }
}
