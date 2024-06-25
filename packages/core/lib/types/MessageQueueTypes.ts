import type { TransactionObservabilityManager } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { PublicHandlerSpy } from '../queues/HandlerSpy'

export interface QueueConsumer {
  start(): Promise<unknown> // subscribe and start listening
  close(): Promise<unknown>
}

export type MessageProcessingResult =
  | 'retryLater'
  | 'consumed'
  | 'published'
  | 'error'
  | 'invalid_message'

export interface SyncPublisher<MessagePayloadType extends object, MessageOptions> {
  handlerSpy: PublicHandlerSpy<MessagePayloadType>
  publish(message: MessagePayloadType, options: MessageOptions): void
}

export interface AsyncPublisher<MessagePayloadType extends object, MessageOptions> {
  handlerSpy: PublicHandlerSpy<MessagePayloadType>
  publish(message: MessagePayloadType, options: MessageOptions): Promise<unknown>
}

export type { TransactionObservabilityManager }

export type LogFn = {
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  <T extends object>(obj: T, msg?: string, ...args: any[]): void
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  (obj: unknown, msg?: string, ...args: any[]): void
  // biome-ignore lint/suspicious/noExplicitAny: This is expected
  (msg: string, ...args: any[]): void
}

export type ExtraParams = {
  logger?: Logger
}

export type Logger = {
  error: LogFn
  info: LogFn
  warn: LogFn
  debug: LogFn
  trace: LogFn
  fatal: LogFn
}

export type SchemaMap<SupportedMessageTypes extends string> = Record<
  SupportedMessageTypes,
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  ZodSchema<any>
>
