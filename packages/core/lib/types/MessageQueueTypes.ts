import type { CommonLogger, TransactionObservabilityManager } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { PublicHandlerSpy } from '../queues/HandlerSpy'

export interface QueueConsumer {
  start(): Promise<unknown> // subscribe and start listening
  close(): Promise<unknown>
}

export type MessageProcessingResult =
  | {
      status: 'retryLater'
    }
  | {
      status: 'consumed' | 'published'
      skippedAsDuplicate?: boolean
    }
  | {
      status: 'error'
      errorReason: 'invalidMessage' | 'handlerError' | 'retryLaterExceeded'
    }
export type MessageProcessingResultStatus = MessageProcessingResult['status']

export interface SyncPublisher<MessagePayloadType extends object, MessageOptions> {
  handlerSpy: PublicHandlerSpy<MessagePayloadType>
  publish(message: MessagePayloadType, options: MessageOptions): void
}

export interface AsyncPublisher<MessagePayloadType extends object, MessageOptions> {
  handlerSpy: PublicHandlerSpy<MessagePayloadType>
  publish(message: MessagePayloadType, options: MessageOptions): Promise<unknown>
}

export type { TransactionObservabilityManager }

export type ExtraParams = {
  logger?: CommonLogger
}

export type SchemaMap<SupportedMessageTypes extends string> = Record<
  SupportedMessageTypes,
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  ZodSchema<any>
>
