export interface Consumer {
  consume(): void
  close(): Promise<void>
}

export interface SyncPublisher<MessagePayloadType> {
  publish(message: MessagePayloadType): void
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
