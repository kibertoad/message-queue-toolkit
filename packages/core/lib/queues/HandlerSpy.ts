import { Fifo } from 'toad-cache'

import type { MessageProcessingResult } from '../types/MessageQueueTypes'

export type HandlerSpyParams = {
  bufferSize?: number
  messageIdField?: string
}

export type SpyResult<MessagePayloadSchemas extends object> = {
  message: MessagePayloadSchemas
  processingResult: MessageProcessingResult
}

export class HandlerSpy<MessagePayloadSchemas extends object> {
  private readonly messageBuffer: Fifo<SpyResult<MessagePayloadSchemas>>
  private readonly messageIdField: string

  constructor(params: HandlerSpyParams = {}) {
    this.messageBuffer = new Fifo(params.bufferSize ?? 100)
    this.messageIdField = params.messageIdField ?? 'id'
  }

  waitForEvent(
    fields: Partial<MessagePayloadSchemas>,
    processingResult?: MessageProcessingResult,
  ): Promise<SpyResult<MessagePayloadSchemas>> {
    // @ts-ignore
      const processedMessageEntry: { value: SpyResult<MessagePayloadSchemas> } = Object.values(this.messageBuffer.items).find((spyResult) => {
      return Object.entries(fields).every(([key, value]) => {
        // @ts-ignore
        return spyResult.value.message[key] === value
      })
    })
    if (processedMessageEntry) {
      return Promise.resolve(processedMessageEntry.value)
    }

    throw new Error('Not found')
  }

  clear() {
    this.messageBuffer.clear()
  }

  addProcessedMessage(processingResult: SpyResult<MessagePayloadSchemas>) {
    // @ts-ignore
    this.messageBuffer.set(processingResult.message[this.messageIdField], processingResult)
  }
}
