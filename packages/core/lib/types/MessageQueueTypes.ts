import type { ZodSchema } from 'zod'

export interface QueueConsumer<MessagePayloadType extends object> {
  start(): Promise<unknown> // subscribe and start listening
  close(): Promise<unknown>
  shouldProcessMessageLater(message: MessagePayloadType, messageType: string): Promise<boolean> // Barrier pattern
}

export interface SyncPublisher<MessagePayloadType> {
  publish(message: MessagePayloadType): void
}

export interface AsyncPublisher<MessagePayloadType, MessageOptions> {
  publish(message: MessagePayloadType, options: MessageOptions): Promise<unknown>
}

export type TransactionObservabilityManager = {
  start: (transactionSpanId: string) => unknown
  stop: (transactionSpanId: string) => unknown
}

export type LogFn = {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  <T extends object>(obj: T, msg?: string, ...args: any[]): void
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (obj: unknown, msg?: string, ...args: any[]): void
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (msg: string, ...args: any[]): void
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ZodSchema<any>
>
