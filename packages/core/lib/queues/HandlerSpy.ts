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

type SpyResultCacheEntry<MessagePayloadSchemas extends object> = {
  value: SpyResult<MessagePayloadSchemas>
}

type SpyPromiseMetadata<MessagePayloadSchemas extends object> = {
  fields: Partial<MessagePayloadSchemas>
  processingResult?: MessageProcessingResult
  promise: Promise<SpyResult<MessagePayloadSchemas>>
  resolve: (
    value: SpyResult<MessagePayloadSchemas> | PromiseLike<SpyResult<MessagePayloadSchemas>>,
  ) => void
}

export class HandlerSpy<MessagePayloadSchemas extends object> {
  private readonly messageBuffer: Fifo<SpyResult<MessagePayloadSchemas>>
  private readonly messageIdField: string
  private readonly spyPromises: SpyPromiseMetadata<MessagePayloadSchemas>[]

  constructor(params: HandlerSpyParams = {}) {
    this.messageBuffer = new Fifo(params.bufferSize ?? 100)
    this.messageIdField = params.messageIdField ?? 'id'
    this.spyPromises = []
  }

  private messageMatchesFilter(
    spyResult: SpyResult<MessagePayloadSchemas>,
    fields: Partial<MessagePayloadSchemas>,
    processingResult?: MessageProcessingResult,
  ) {
    return (
      Object.entries(fields).every(([key, value]) => {
        // @ts-ignore
        return spyResult.message[key] === value
      }) &&
      (!processingResult || spyResult.processingResult === processingResult)
    )
  }

  waitForEvent(
    fields: Partial<MessagePayloadSchemas>,
    processingResult?: MessageProcessingResult,
  ): Promise<SpyResult<MessagePayloadSchemas>> {
    const processedMessageEntry = Object.values(this.messageBuffer.items).find(
      // @ts-ignore
      (spyResult: SpyResultCacheEntry<MessagePayloadSchemas>) => {
        return this.messageMatchesFilter(spyResult.value, fields, processingResult)
      },
    )
    if (processedMessageEntry) {
      return Promise.resolve(processedMessageEntry.value)
    }

    let resolve: (
      value: SpyResult<MessagePayloadSchemas> | PromiseLike<SpyResult<MessagePayloadSchemas>>,
    ) => void
    const spyPromise = new Promise<SpyResult<MessagePayloadSchemas>>((_resolve) => {
      resolve = _resolve
    })

    this.spyPromises.push({
      promise: spyPromise,
      processingResult,
      fields,
      // @ts-ignore
      resolve,
    })

    return spyPromise
  }

  clear() {
    this.messageBuffer.clear()
  }

  addProcessedMessage(processingResult: SpyResult<MessagePayloadSchemas>) {
    // @ts-ignore
    const cacheId = `${processingResult.message[this.messageIdField]}-${Date.now()}-${(
      Math.random() + 1
    )
      .toString(36)
      .substring(7)}`
    this.messageBuffer.set(cacheId, processingResult)

    const foundPromise = this.spyPromises.find((spyPromise) => {
      return this.messageMatchesFilter(
        processingResult,
        spyPromise.fields,
        spyPromise.processingResult,
      )
    })

    if (foundPromise) {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call
      foundPromise.resolve(processingResult)

      const index = this.spyPromises.indexOf(foundPromise)
      if (index > -1) {
        // only splice array when item is found
        this.spyPromises.splice(index, 1) // 2nd parameter means remove one item only
      }
    }
  }
}
