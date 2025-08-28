import { randomUUID } from 'node:crypto'

import { isObject } from '@lokalise/node-core'
import { Fifo } from 'toad-cache'

import type {
  MessageProcessingResult,
  MessageProcessingResultStatus,
} from '../types/MessageQueueTypes.ts'
import { objectMatches } from '../utils/matchUtils.ts'

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

type SpyPromiseMetadata<MessagePayloadSchemas extends object> = {
  fields: DeepPartial<MessagePayloadSchemas>
  status?: MessageProcessingResultStatus
  promise: Promise<SpyResultOutput<MessagePayloadSchemas>>
  resolve: (
    value:
      | SpyResultInput<MessagePayloadSchemas>
      | PromiseLike<SpyResultOutput<MessagePayloadSchemas>>,
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

// biome-ignore lint/complexity/noBannedTypes: Expected
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
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  private readonly messageBuffer: Fifo<SpyResultInput<any>>
  private readonly messageIdField: keyof MessagePayloadSchemas
  private readonly messageTypeField: keyof MessagePayloadSchemas
  private readonly spyPromises: SpyPromiseMetadata<MessagePayloadSchemas>[]

  constructor(params: HandlerSpyParams = {}) {
    this.messageBuffer = new Fifo(params.bufferSize ?? 100)
    // @ts-expect-error
    this.messageIdField = params.messageIdField ?? 'id'
    // @ts-expect-error
    this.messageTypeField = params.messageTypeField ?? 'type'
    this.spyPromises = []
  }

  private messageMatchesFilter<T extends object>(
    spyResult: SpyResultOutput<T>,
    fields: DeepPartial<MessagePayloadSchemas>,
    status?: MessageProcessingResultStatus,
  ): boolean {
    return (
      objectMatches(fields, spyResult.message) &&
      (!status || spyResult.processingResult.status === status)
    )
  }

  waitForMessageWithId<T extends MessagePayloadSchemas>(
    id: string,
    status?: MessageProcessingResultStatus,
  ): Promise<SpyResultOutput<T>> {
    return this.waitForMessage<T>(
      // @ts-expect-error
      { [this.messageIdField]: id },
      status,
    )
  }

  checkForMessage<T extends MessagePayloadSchemas>(
    expectedFields: DeepPartial<T>,
    status?: MessageProcessingResultStatus,
  ): SpyResultOutput<T> | undefined {
    return Object.values(this.messageBuffer.items).find((spyResult) => {
      return this.messageMatchesFilter(spyResult.value, expectedFields, status)
    })?.value
  }

  waitForMessage<T extends MessagePayloadSchemas>(
    expectedFields: DeepPartial<T>,
    status?: MessageProcessingResultStatus,
  ): Promise<SpyResultOutput<T>> {
    const processedMessageEntry = this.checkForMessage(expectedFields, status)
    if (processedMessageEntry) {
      return Promise.resolve(processedMessageEntry)
    }

    let resolve: (value: SpyResultOutput<T> | PromiseLike<SpyResultOutput<T>>) => void
    const spyPromise = new Promise<SpyResultOutput<T>>((_resolve) => {
      resolve = _resolve
    })

    this.spyPromises.push({
      promise: spyPromise,
      status,
      fields: expectedFields,
      // @ts-expect-error
      resolve,
    })

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
      ? (processingResult as SpyResultOutput<MessagePayloadSchemas>)
      : ({
          ...processingResult,
          message: {
            [this.messageIdField]: messageId,
            [this.messageTypeField]: resolvedMessageType,
          },
        } as SpyResultOutput<MessagePayloadSchemas>)

    const cacheId = `${resolvedMessageId}-${Date.now()}-${(Math.random() + 1)
      .toString(36)
      .substring(7)}`
    this.messageBuffer.set(cacheId, resolvedProcessingResult)

    const foundPromise = this.spyPromises.find((spyPromise) => {
      return this.messageMatchesFilter(
        resolvedProcessingResult,
        spyPromise.fields,
        spyPromise.status,
      )
    })

    if (foundPromise) {
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
