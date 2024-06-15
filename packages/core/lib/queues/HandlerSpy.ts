import { randomUUID } from 'node:crypto'

import { isObject } from '@lokalise/node-core'
import { Fifo } from 'toad-cache'

import type { MessageProcessingResult } from '../types/MessageQueueTypes'
import { objectMatches } from '../utils/matchUtils'

export type HandlerSpyParams = {
  bufferSize?: number
  messageIdField?: string
  messageTypeField?: string
}

export type SpyResultInput<MessagePayloadSchemas extends object> = {
  message: MessagePayloadSchemas | null
  processingResult: MessageProcessingResult
}

export type SpyResultOutput<MessagePayloadSchemas extends object> = {
  message: MessagePayloadSchemas
  processingResult: MessageProcessingResult
}

type SpyResultCacheEntry<MessagePayloadSchemas extends object> = {
  value: SpyResultInput<MessagePayloadSchemas>
}

type SpyPromiseMetadata<MessagePayloadSchemas extends object> = {
  fields: DeepPartial<MessagePayloadSchemas>
  processingResult?: MessageProcessingResult
  promise: Promise<SpyResultInput<MessagePayloadSchemas>>
  resolve: (
    value:
      | SpyResultInput<MessagePayloadSchemas>
      | PromiseLike<SpyResultInput<MessagePayloadSchemas>>,
  ) => void
}

export function isHandlerSpy<T extends object>(value: unknown): value is HandlerSpy<T> {
  return (
    isObject(value) &&
    (value instanceof HandlerSpy || (value as unknown as HandlerSpy<object>).name === 'HandlerSpy')
  )
}

export type PublicHandlerSpy<MessagePayloadSchemas extends object> = Omit<
  HandlerSpy<MessagePayloadSchemas>,
  'addProcessedMessage'
>

// eslint-disable-next-line @typescript-eslint/ban-types
type DeepPartial<T> = T extends Function
  ? T
  : T extends object
    ? {
        [P in keyof T]?: T[P] extends Array<infer U>
          ? Array<DeepPartial<U>>
          : T[P] extends ReadonlyArray<infer U>
            ? ReadonlyArray<DeepPartial<U>>
            : DeepPartial<T[P]>
      }
    : T

export class HandlerSpy<MessagePayloadSchemas extends object> {
  public name = 'HandlerSpy'
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly messageBuffer: Fifo<SpyResultInput<any>>
  private readonly messageIdField: keyof MessagePayloadSchemas
  private readonly messageTypeField: keyof MessagePayloadSchemas
  private readonly spyPromises: SpyPromiseMetadata<MessagePayloadSchemas>[]

  constructor(params: HandlerSpyParams = {}) {
    this.messageBuffer = new Fifo(params.bufferSize ?? 100)
    // @ts-ignore
    this.messageIdField = params.messageIdField ?? 'id'
    // @ts-ignore
    this.messageTypeField = params.messageTypeField ?? 'type'
    this.spyPromises = []
  }

  private messageMatchesFilter(
    spyResult: SpyResultInput<object>,
    fields: DeepPartial<MessagePayloadSchemas>,
    processingResult?: MessageProcessingResult,
  ) {
    return (
      // @ts-ignore
      objectMatches(fields, spyResult.message) &&
      (!processingResult || spyResult.processingResult === processingResult)
    )
  }

  waitForMessageWithId<T extends MessagePayloadSchemas>(
    id: string,
    processingResult?: MessageProcessingResult,
  ): Promise<SpyResultOutput<T>> {
    return this.waitForMessage<T>(
      // @ts-ignore
      {
        [this.messageIdField]: id,
      },
      processingResult,
    )
  }

  waitForMessage<T extends MessagePayloadSchemas>(
    fields: DeepPartial<T>,
    processingResult?: MessageProcessingResult,
  ): Promise<SpyResultOutput<T>> {
    const processedMessageEntry = Object.values(this.messageBuffer.items).find(
      // @ts-ignore
      (spyResult: SpyResultCacheEntry<T>) => {
        return this.messageMatchesFilter(spyResult.value, fields, processingResult)
      },
    )
    if (processedMessageEntry) {
      return Promise.resolve(processedMessageEntry.value)
    }

    let resolve: (value: SpyResultInput<T> | PromiseLike<SpyResultInput<T>>) => void
    const spyPromise = new Promise<SpyResultInput<T>>((_resolve) => {
      resolve = _resolve
    })

    this.spyPromises.push({
      promise: spyPromise,
      processingResult,
      fields,
      // @ts-ignore
      resolve,
    })

    // @ts-ignore
    return spyPromise
  }

  clear() {
    this.messageBuffer.clear()
  }

  addProcessedMessage(processingResult: SpyResultInput<MessagePayloadSchemas>, messageId?: string) {
    const resolvedMessageId =
      processingResult.message?.[this.messageIdField] ?? messageId ?? randomUUID()

    const resolvedMessageType =
      processingResult.message?.[this.messageTypeField] ?? 'FAILED_TO_RESOLVE'

    // If we failed to parse message, let's store id and type at least
    const resolvedProcessingResult = processingResult.message
      ? processingResult
      : {
          ...processingResult,
          message: {
            [this.messageIdField]: messageId,
            [this.messageTypeField]: resolvedMessageType,
          },
        }

    // @ts-ignore
    const cacheId = `${resolvedMessageId}-${Date.now()}-${(Math.random() + 1)
      .toString(36)
      .substring(7)}`
    this.messageBuffer.set(cacheId, resolvedProcessingResult)

    const foundPromise = this.spyPromises.find((spyPromise) => {
      return this.messageMatchesFilter(
        resolvedProcessingResult,
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

export function resolveHandlerSpy<T extends object>(queueOptions: {
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
}) {
  if (isHandlerSpy(queueOptions.handlerSpy)) {
    return queueOptions.handlerSpy as unknown as HandlerSpy<T>
  }
  if (!queueOptions.handlerSpy) {
    return undefined
  }
  if (queueOptions.handlerSpy === true) {
    return new HandlerSpy() as unknown as HandlerSpy<T>
  }

  return new HandlerSpy(queueOptions.handlerSpy) as unknown as HandlerSpy<T>
}
